package dk.ku.di.dms.vms.modb.query.planner;

import dk.ku.di.dms.vms.modb.common.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.definition.ColumnReference;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.CompositeKey;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.SimpleKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.GroupByPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractOperator;
import dk.ku.di.dms.vms.modb.query.planner.operators.count.IndexCount;
import dk.ku.di.dms.vms.modb.query.planner.operators.count.IndexCountGroupBy;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.AbstractScan;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.FullScanWithProjection;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.IndexScanWithProjection;
import dk.ku.di.dms.vms.modb.query.planner.operators.sum.IndexSum;
import dk.ku.di.dms.vms.modb.query.planner.operators.sum.Sum;

import java.util.List;

/**
 * Planner that only takes into consideration simple queries.
 * Those involving single tables and filters, and no
 * aggregation or joins.
 */
public class Planner {

    public Planner(){}

    public AbstractOperator plan(QueryTree queryTree) {

        if(queryTree.isSimpleScan()){
            return planSimpleSelect(queryTree);
        }

        if(queryTree.isSimpleAggregate()){
            return planSimpleAggregate(queryTree);
        }

        return null;

    }

    private AbstractOperator planSimpleAggregate(QueryTree queryTree) {

        //
        // Table tb = queryTree.groupByProjections.get(0).columnReference.table;

        // get the operations
        // groupby selecti
        if(!queryTree.groupByProjections.isEmpty()){
            // then just one since it is simple
            switch (queryTree.groupByProjections.get(0).groupByOperation){
                case SUM -> {
                    // is there any index that applies?
                    AbstractIndex<IKey> indexSelected = getOptimalHashIndex(
                            queryTree.wherePredicates,
                            queryTree.groupByProjections.get(0).columnReference.table);
                    if(indexSelected == null){
                        return new Sum(queryTree.groupByProjections.get(0).columnReference.dataType,
                                queryTree.groupByProjections.get(0).columnReference.columnPosition,
                                queryTree.groupByProjections.get(0).columnReference.table.primaryKeyIndex);
                    }
                    return new IndexSum(queryTree.groupByProjections.get(0).columnReference.dataType,
                            queryTree.groupByProjections.get(0).columnReference.columnPosition,
                            indexSelected);
                }
                case COUNT -> {
                    Table tb = queryTree.groupByProjections.get(0).columnReference.table;
                    AbstractIndex<IKey> indexSelected = getOptimalHashIndex(
                            queryTree.wherePredicates,
                            queryTree.groupByProjections.get(0).columnReference.table);
                    if(queryTree.groupByColumns.isEmpty()){
                        // then no group by

                        // how the user can specify a distinct?

                        return new IndexCount( indexSelected == null ? tb.primaryKeyIndex() : indexSelected );


                    } else {
                        int[] columns = queryTree.groupByColumns.stream()
                                .mapToInt(ColumnReference::getColumnPosition ).toArray();
                        return new IndexCountGroupBy( indexSelected == null ? tb.primaryKeyIndex() : indexSelected, columns );
                    }

                }
            }
        } else {
            // just grouping of rows? tuples then may repeat for each group
        }

        // get the columns that must be considered for the aggregations

        // GroupByPredicate predicate = queryTree.groupByProjections.get(0).
        return null;
    }

    /**
     *
     * @param queryTree
     */
    private AbstractScan planSimpleSelect(QueryTree queryTree) {

        // given it is simple, pick the table from one of the columns
        // must always have at least one projected column
        Table tb = queryTree.projections.get(0).table;

        // avoid one of the columns to have expression different from EQUALS
        // to be picked by unique and non unique index
        AbstractIndex<IKey> indexSelected = getOptimalHashIndex(queryTree.wherePredicates, tb);

        // build projection

        // compute before creating this. compute in startup
        int nProj = queryTree.projections.size();
        int[] projectionColumns = new int[nProj];
        int[] valueSizeInBytes = new int[nProj];
        int entrySize = 0;
        for(int i = 0; i < nProj; i++){
            projectionColumns[i] = queryTree.projections.get(i).columnPosition;
            valueSizeInBytes[i] = indexSelected
                    .getTable().getSchema()
                    .getColumnDataType( queryTree.projections.get(i).columnPosition ).value;
            entrySize += valueSizeInBytes[i];
        }

        if(indexSelected != null) {
            // return the indexscanwithprojection
            return new IndexScanWithProjection(indexSelected, projectionColumns, valueSizeInBytes, entrySize);

        } else {
            // then must get the PK index, ScanWithProjection
            return new FullScanWithProjection( tb.primaryKeyIndex, projectionColumns, valueSizeInBytes, entrySize );

        }

    }

    private AbstractIndex<IKey> getOptimalHashIndex(List<WherePredicate> wherePredicates, Table tb) {
        int[] filterColumns = wherePredicates.stream()
                 .filter( wherePredicate -> wherePredicate.expression == ExpressionTypeEnum.EQUALS )
                 .mapToInt( WherePredicate::getColumnPosition ).toArray();

        AbstractIndex<IKey> indexSelected = pickIndex(tb, filterColumns);
        return indexSelected;
    }

    private AbstractIndex<IKey> pickIndex(Table table, int[] filterColumns){

        IKey indexKey;
        if(filterColumns.length == 1) {
            indexKey = SimpleKey.of(filterColumns[0]);
        } else {
            indexKey = CompositeKey.of(filterColumns);
        }

        if (table.primaryKeyIndex().hashCode() == indexKey.hashCode()) {
            return table.primaryKeyIndex();
        }

        if(table.indexes.get(indexKey) != null){
            return table.indexes.get(indexKey);
        }

        // no index apply so far, perhaps a subset then?
        List<int[]> combinations = Combinatorics.getAllPossibleColumnCombinations(filterColumns);

        // heuristic: return the one that embraces more columns
        AbstractIndex<IKey> bestSoFar = null;
        int maxLength = 0;
        for(int[] arr : combinations) {

            if (arr.length == 1) {
                indexKey = SimpleKey.of(filterColumns[0]);
            } else {
                indexKey = CompositeKey.of(filterColumns);
            }

            if(table.indexes.get(indexKey) != null){
                if(arr.length > maxLength){
                    bestSoFar = table.indexes.get(indexKey);
                    maxLength = arr.length;
                }
            }

        }

        return bestSoFar;

    }

}