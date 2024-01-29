package dk.ku.di.dms.vms.modb.query.planner;

import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.definition.ColumnReference;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.CompositeKey;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.SimpleKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyBufferIndex;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.JoinPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.query.execution.operators.AbstractSimpleOperator;
import dk.ku.di.dms.vms.modb.query.execution.operators.count.IndexCount;
import dk.ku.di.dms.vms.modb.query.execution.operators.count.IndexCountGroupBy;
import dk.ku.di.dms.vms.modb.query.execution.operators.join.UniqueHashJoinNonUniqueHashWithProjection;
import dk.ku.di.dms.vms.modb.query.execution.operators.join.UniqueHashJoinWithProjection;
import dk.ku.di.dms.vms.modb.query.execution.operators.scan.AbstractScan;
import dk.ku.di.dms.vms.modb.query.execution.operators.scan.FullScanWithProjection;
import dk.ku.di.dms.vms.modb.query.execution.operators.scan.IndexScanWithProjection;
import dk.ku.di.dms.vms.modb.query.execution.operators.sum.IndexSum;
import dk.ku.di.dms.vms.modb.query.execution.operators.sum.Sum;

import java.util.List;
import java.util.stream.IntStream;

/**
 * Planner that only takes into consideration simple read and write queries.
 * Those involving single tables and filters, and no
 * multiple aggregations or joins with more than 2 tables.
 */
public final class SimplePlanner {

    public SimplePlanner(){}

    /**
     * @param columnsToFilter additional columns to be filtered, but not on index
     */
    private record IndexSelectionVerdict(boolean indexIsUsedGivenWhereClause,
                                         ReadWriteIndex<IKey> index,
                                         int[] columnsToFilter) implements Comparable<IndexSelectionVerdict> {

        @Override
        public int compareTo(IndexSelectionVerdict o) {
            // do both indexes can be effectively applied?
            if (this.indexIsUsedGivenWhereClause && o.indexIsUsedGivenWhereClause) {

                // are both unique?
                if (this.index.getType() == IndexTypeEnum.UNIQUE && o.index.getType() == IndexTypeEnum.UNIQUE) {
                    // which one has more columns to filter? leading to more rows cut
                    // that may not be correct given the selectivity of some values, simple heuristic here
                    if (this.columnsToFilter.length > o.columnsToFilter.length) return 0;
                    if (this.columnsToFilter.length < o.columnsToFilter.length) return 1;

                    // which one has larger table?
                    if (this.index.size() < o.index.size()) return 0;

                } else if (this.index.getType() == IndexTypeEnum.UNIQUE) return 0;

            } else if (this.indexIsUsedGivenWhereClause) {
                return 0; // must be deep left then
            }
            return 1;
        }

    }

    public AbstractSimpleOperator plan(QueryTree queryTree) {

        if(queryTree.isSimpleScan()){
            return this.planSimpleSelect(queryTree);
        }

        if(queryTree.isSimpleAggregate()){
            return this.planSimpleAggregate(queryTree);
        }

        if(queryTree.isSimpleJoin()){
            return this.planSimpleJoin(queryTree);
        }

        if(queryTree.hasMultipleJoins()){
            return this.planMultipleJoinsQuery(queryTree);
        }

        return null;

    }

    private AbstractSimpleOperator planSimpleJoin(QueryTree queryTree) {

        // define left deep
        JoinPredicate joinPredicate = queryTree.joinPredicates.get(0);

        // the index with most columns applying to the probe is the deepest
        IndexSelectionVerdict indexForTable1 = this.getOptimalIndex(joinPredicate.getLeftTable(), queryTree.wherePredicates);

        IndexSelectionVerdict indexForTable2 = this.getOptimalIndex(joinPredicate.getRightTable(), queryTree.wherePredicates);

        // TODO finish

        if(indexForTable1.compareTo( indexForTable2 ) == 0){
            // should be left
            if(indexForTable1.index.getType() == IndexTypeEnum.UNIQUE && indexForTable2.index.getType() == IndexTypeEnum.UNIQUE)
                return new UniqueHashJoinWithProjection( indexForTable1.index, indexForTable2.index,
                    null, null,
                    null, null,
                    null, 0 );
            if(indexForTable1.index.getType() == IndexTypeEnum.UNIQUE)
                return new UniqueHashJoinNonUniqueHashWithProjection( indexForTable1.index, indexForTable2.index,
                        null, null,
                        null, null,
                        null, 0 );
            throw new IllegalStateException("No support for join on non unique hash indexes!");
        } else {
            if(indexForTable2.index.getType() == IndexTypeEnum.UNIQUE && indexForTable1.index.getType() == IndexTypeEnum.UNIQUE)
                return new UniqueHashJoinWithProjection( indexForTable2.index, indexForTable1.index,
                    null, null,
                    null, null,
                    null, 0 );
            if(indexForTable2.index.getType() == IndexTypeEnum.UNIQUE)
                return new UniqueHashJoinNonUniqueHashWithProjection( indexForTable2.index, indexForTable1.index,
                        null, null,
                        null, null,
                        null, 0 );
            throw new IllegalStateException("No support for join on non unique hash indexes!");
        }

    }

    private AbstractSimpleOperator planMultipleJoinsQuery(QueryTree queryTree) {

        // order joins in order of join operation
        // simple heuristic, table with most records goes first, but care must be taken on precedence of foreign keys

        // dynamic programming. which join must execute first?
        // ordered by the number of records? index type?

        return null;

    }

    private AbstractSimpleOperator planSimpleAggregate(QueryTree queryTree) {

        //
        // Table tb = queryTree.groupByProjections.get(0).columnReference.table;

        // get the operations
        // group by selection

        // then just one since it is simple
        switch (queryTree.groupByProjections.get(0).groupByOperation){
            case SUM -> {
                // is there any index that applies?
                ReadWriteIndex<IKey> indexSelected = this.getOptimalIndex(
                        queryTree.groupByProjections.get(0).columnReference.table,
                        queryTree.wherePredicates
                        ).index;
                if(indexSelected == null){
                    return new Sum(queryTree.groupByProjections.get(0).columnReference.dataType,
                            queryTree.groupByProjections.get(0).columnReference.columnPosition,
                            queryTree.groupByProjections.get(0).columnReference.table.underlyingPrimaryKeyIndex());
                }
                return new IndexSum(queryTree.groupByProjections.get(0).columnReference.dataType,
                        queryTree.groupByProjections.get(0).columnReference.columnPosition,
                        indexSelected);
            }
            case COUNT -> {
                Table tb = queryTree.groupByProjections.get(0).columnReference.table;
                ReadOnlyIndex<IKey> indexSelected = this.getOptimalIndex(
                        queryTree.groupByProjections.get(0).columnReference.table,
                        queryTree.wherePredicates
                        ).index;
                if(queryTree.groupByColumns.isEmpty()){
                    // then no group by

                    // how the user can specify a distinct?
                    return new IndexCount( indexSelected == null ? tb.underlyingPrimaryKeyIndex() : indexSelected );

                } else {
                    int[] columns = queryTree.groupByColumns.stream()
                            .mapToInt(ColumnReference::getColumnPosition ).toArray();
                    return new IndexCountGroupBy( indexSelected == null ? tb.underlyingPrimaryKeyIndex() : indexSelected, columns );
                }

            }
            default -> throw new IllegalStateException("Operator not yet implemented.");
        }


        // get the columns that must be considered for the aggregations

        // GroupByPredicate predicate = queryTree.groupByProjections.get(0).

    }

    /**
     */
    private AbstractScan planSimpleSelect(QueryTree queryTree) {

        // given it is simple, pick the table from one of the columns
        // must always have at least one projected column
        Table tb = queryTree.projections.get(0).table;

        // avoid one of the columns to have expression different from EQUALS
        // to be picked by unique and non unique index
        ReadOnlyIndex<IKey> indexSelected = this.getOptimalIndex(tb, queryTree.wherePredicates).index();

        // build projection

        // compute before creating this. compute in startup
        int nProj = queryTree.projections.size();
        int[] projectionColumns = new int[nProj];
        int entrySize = 0;
        for(int i = 0; i < nProj; i++){
            projectionColumns[i] = queryTree.projections.get(i).columnPosition;
            entrySize += indexSelected.schema()
                    .columnDataType( queryTree.projections.get(i).columnPosition ).value;
        }

        if(indexSelected != null) {
            // return the index scan with projection
            return new IndexScanWithProjection(tb, indexSelected, projectionColumns, entrySize);

        } else {
            // then must get the PK index, ScanWithProjection
            return new FullScanWithProjection( tb, tb.underlyingPrimaryKeyIndex(), projectionColumns, entrySize );

        }

    }

    public IndexSelectionVerdict getOptimalIndex(final Table table, List<WherePredicate> wherePredicates) {

        final IntStream intStream = wherePredicates.stream()
                .filter( wherePredicate ->
                        wherePredicate.expression == ExpressionTypeEnum.EQUALS
                                && wherePredicate.columnReference.table.equals(table)
                )
                .mapToInt( WherePredicate::getColumnPosition );

        final int[] columnsForIndexSelection = intStream.toArray();

        final IKey indexKey;
        if(columnsForIndexSelection.length == 1) {
            indexKey = SimpleKey.of(columnsForIndexSelection[0]);
        } else {
            indexKey = CompositeKey.of(columnsForIndexSelection);
        }

        // fast path (1): all columns are part of the primary index
        if (table.underlyingPrimaryKeyIndex().key().equals(indexKey) ) {
            // int[] filterColumns = intStream.filter( w -> !table.underlyingPrimaryKeyIndex_().containsColumn( w ) ).toArray();
            return new IndexSelectionVerdict(true, table.underlyingPrimaryKeyIndex(), columnsForIndexSelection);
        }

        // fast path (2): all columns are part of a secondary index
        if(table.secondaryIndexMap.get(indexKey) != null){
            // int[] filterColumns = Arrays.stream(columnsForIndexSelection).filter( w -> !table.underlyingPrimaryKeyIndex_().containsColumn( w ) ).toArray();
            return new IndexSelectionVerdict(true,

                    table.secondaryIndexMap.get(indexKey).getUnderlyingIndex(),
                    columnsForIndexSelection);
        }

        final ReadOnlyBufferIndex<IKey> indexSelected = this.getOptimalIndex(table, columnsForIndexSelection);

        // is the index completely covered by the columns in the filter?
        if (indexSelected == null){
            // then just select the Primary index
            return new IndexSelectionVerdict(false, table.underlyingPrimaryKeyIndex(), columnsForIndexSelection);
        }

        // columns not in the index, but require filtering
        final IntStream filteredStream = intStream.filter( w -> !indexSelected.containsColumn( w ) );
        final int[] filterColumns = filteredStream.toArray();

        // any column of the index is in the filter? if so, index is not used.
        boolean indexColumnInFilter = filteredStream.anyMatch(indexSelected::containsColumn);

        return new IndexSelectionVerdict(indexColumnInFilter, table.underlyingPrimaryKeyIndex(), filterColumns);
    }

    @SuppressWarnings("unchecked")
    private ReadOnlyBufferIndex<IKey> getOptimalIndex(Table table, int[] filterColumns){

        IKey indexKey;

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

            if(table.secondaryIndexMap.get(indexKey) != null){
                if(arr.length > maxLength){
                    bestSoFar = table.secondaryIndexMap.get(indexKey).getUnderlyingIndex();
                    maxLength = arr.length;
                }
            }

        }

        return (ReadOnlyBufferIndex<IKey>) bestSoFar;

    }

}