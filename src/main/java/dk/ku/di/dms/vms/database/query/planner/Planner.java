package dk.ku.di.dms.vms.database.query.planner;

import dk.ku.di.dms.vms.database.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.database.query.analyzer.predicate.JoinPredicate;
import dk.ku.di.dms.vms.database.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionEnum;
import dk.ku.di.dms.vms.database.query.planner.node.filter.FilterInfo;
import dk.ku.di.dms.vms.database.query.planner.node.filter.IFilter;
import dk.ku.di.dms.vms.database.query.planner.node.filter.FilterBuilder;
import dk.ku.di.dms.vms.database.query.planner.node.scan.SequentialScan;
import dk.ku.di.dms.vms.database.query.planner.utils.IdentifiableNode;
import dk.ku.di.dms.vms.database.store.index.AbstractIndex;
import dk.ku.di.dms.vms.database.store.index.IndexTypeEnum;
import dk.ku.di.dms.vms.database.store.meta.Schema;
import dk.ku.di.dms.vms.database.store.table.Table;

import java.util.*;
import java.util.stream.Collectors;

public final class Planner {

    // https://www.interdb.jp/pg/pgsql03.html
    // "The planner receives a query tree from the rewriter and generates a (query)
    // plan tree that can be processed by the executor most effectively."

    // plan and optimize in the same step
    public PlanNode plan(final QueryTree queryTree) throws Exception {

        // https://github.com/hyrise/hyrise/tree/master/src/lib/operators
        // https://github.com/hyrise/hyrise/tree/master/src/lib/optimizer
        // https://github.com/hyrise/hyrise/tree/master/src/lib/logical_query_plan

        // define the steps, indexes to use, do we need to sort, limit, group?....
        // define the rules to apply based on the set of pre-defined rules

        // join rule always prune the shortest relation...
        // always apply the filter first
        // do I have any matching index?
        // do we have join? what is the outer table?
        // what should come first, the join or the filter. should I apply the
        // filter together with the join or before? cost optimization
        // native main memory DBMS for data-intensive applications
        // many layers orm introduces, codification and de-codification
        // of application-defined objects into the underlying store. pays a performance price
        // at the same time developers challenge with caching mechanisms. that should be handled natively
        // by a MMDBMS

        // group the where clauses by table
        // https://stackoverflow.com/questions/40172551/static-context-cannot-access-non-static-in-collectors
//        Map<Table<?,?>,List<Column>> columnsWhereClauseGroupedByTable =
//                queryTree
//                .whereClauses.stream()
//                .collect(
//                        Collectors.groupingBy( WhereClause::getTable,
//                        Collectors.mapping( WhereClause::getColumn, Collectors.toList()))
//                );

        final Map<String,Integer> tablesInvolvedInJoin = new Hashtable<>();
        queryTree
                .joinPredicates
                .stream()
                .map(j-> j.columnLeftReference.table)
                .forEach( tb ->
                        tablesInvolvedInJoin.put(
                                tb.getName(),1
                        ) );

        queryTree.joinPredicates.stream()
                .map(j-> j.columnRightReference.table)
                .forEach( tb -> tablesInvolvedInJoin.put(
                        tb.getName(),1
                ));

        // cartesian product
        // select tb3.id, tb1.id, tb2.id from tb1, tb2, tb3 where tb1.id = tb2.id and tb1.io = 1

        final Map<Table,List<WherePredicate>> whereClauseGroupedByTable =
                queryTree
                        .wherePredicates.stream()
                        .filter( clause -> !tablesInvolvedInJoin
                                .containsKey(clause.getTable().getName()) )
                        .collect(
                                Collectors.groupingBy( WherePredicate::getTable,
                                         Collectors.toList())
                        );

        Map<Table,List<WherePredicate>> filtersForJoinGroupedByTable =
                queryTree
                        .wherePredicates.stream()
                        .filter( clause -> tablesInvolvedInJoin
                                .containsKey(clause.getTable().getName()) )
                        .collect(
                                Collectors.groupingBy( WherePredicate::getTable,
                                        Collectors.toList())
                        );

        // right, what indexes can I use?
        // but first, there are indexes available?

        // combinatorics to see which index could be applied?
        // is there any dynamic programming algorithm for that?

        // TODO annotate entities with index and read the index annotations
        // https://www.baeldung.com/jpa-indexes
        // https://dba.stackexchange.com/questions/253037/postgres-using-seq-scan-with-filter-on-indexed-column-exists-on-related-table

        // naive, check joins, do I have an index? if so, use it, otherwise embrace the where clause during the join


        // TODO FINISH usually the projection is pushed down in disk-based DBMS,
        //  but here I will just create the final projection when I need to
        //  deliver the result back to the application

        // TODO optimization. the same table can appear in several joins.
        //  in this sense, which join + filter should be executed first to give the best prune of records?
        for(final JoinPredicate joinPredicate : queryTree.joinPredicates){

            // get filters for this join

            Table tableLeft = joinPredicate.getLeftTable();

            // first the left table
            // List<IFilter<?>>

            Table tableRight = joinPredicate.getRightTable();



            // TODO the shortest the table, lower the degree of the join operation in the query tree

        }

        // ordered list. better selectivity rate leads to a deeper node

        // TODO can we merge the filters with the join? i think so. after building the join operators, we can do it
        // remove from map after applying the additional filters
//            filtersForJoinGroupedByTable

        // Map<Table,List<IJoin>>

        // the scan strategy only applies for those tables not involved in any join

        // a sequential scan for each table
        // TODO all predicates with the same column should be a single filter
        for( final Map.Entry<Table, List<WherePredicate>> entry : whereClauseGroupedByTable.entrySet() ){

            final List<WherePredicate> wherePredicates = entry.getValue();
            final int size = wherePredicates.size();
            IFilter<?>[] filters = new IFilter<?>[size];
            int[] filterColumns = new int[size];
            Collection<IdentifiableNode<Object>> filterParams = new LinkedList<>();

            Table currTable = entry.getKey();

            int i = 0;
            for(final WherePredicate w : wherePredicates){

                boolean nullable = w.expression == ExpressionEnum.IS_NOT_NULL || w.expression == ExpressionEnum.IS_NULL;

                filters[ i ] = FilterBuilder.build( w );
                filterColumns[ i ] = w.columnReference.columnIndex;
                if( !nullable ){
                    filterParams.add( new IdentifiableNode<>( i, w.value ) );
                }

                i++;
            }

            // TODO is there any index that can help? from all the indexes, which one gives the best selectivity?
            // actually I need the index information to decide which operator, e.g., index scan


            // Here we are relying on the fact that every index created are stored in order of the table column

            final FilterInfo filterInfo = new FilterInfo(filters, filterColumns, filterParams);
            final AbstractIndex optimalIndex = findOptimalIndex( currTable, filterInfo);
            final SequentialScan seqScan = new SequentialScan(optimalIndex,filterInfo);



        }

        // TODO focus on left deep, most cases will fall in this category

        // TODO think about parallel seq scan,
        // SequentialScan sequentialScan = new SequentialScan( filterList, table);

        return null;
    }

    private AbstractIndex findOptimalIndex(final Table table, final FilterInfo filterInfo){

        // all combinations... TODO this can be built in the previous step...
        final List<int[]> combinations = getAllPossibleColumnCombinations(filterInfo.filterColumns);

        // TODO this can be done inside getAllPossibleColumnCombinations
        // build map of hash code
        Map<Integer,int[]> hashCodeMap = new HashMap<>(combinations.size());

        for(final int[] arr : combinations){
            hashCodeMap.put( Arrays.hashCode( arr ), arr );
        }

        // for each index, check if the column set matches
        float bestSelectivity = Float.MAX_VALUE;
        AbstractIndex optimalIndex = null;

        // first try the primary index

        if(table.hasPrimaryKey()){
            final AbstractIndex pIndex = table.getPrimaryIndex();
            if( hashCodeMap.get( pIndex.hashCode() ) != null ){
                // bestSelectivity = 1D / table.size();
                return pIndex;
            }
        }

        // the selectivity may be unknown, we should use heuristic here to decide what is the best index
        // TODO we could insert a selectivity collector in the sequential scan so later this data can be used here
        for( final AbstractIndex secIndex : table.getSecondaryIndexes() ){

            /*
             * in case selectivity is not present,
             * then we use size the type of the column, e.g., int > long > float > double > String
             *  https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html
             */
            // for this algorithm I am only considering exact matches, so range indexes >,<,>=,=< are not included for now
            int[] columns = hashCodeMap.getOrDefault( secIndex.hashCode(), null );
            if( columns != null ){

                float totalSize = 0;
                final Schema schema = table.getSchema();
                for(int iCol = 0; iCol < columns.length; iCol++){
                    switch ( schema.getColumnDataType( iCol ) ){
                        case INT:
                        case FLOAT:
                            totalSize += 32; break;
                        case STRING: totalSize += 16 * 8; break;
                        // 16-bit * average number of chars in a string. average size of value in a given column would be the ideal
                        case LONG:
                        case DOUBLE:
                            totalSize += 64; break;
                        case CHAR: totalSize += 16; break;
                    }

                }

                // try next index then
                if(totalSize > bestSelectivity) continue;

                // if tiebreak, hash index type is chosen as the tiebreaker criterion

                if(totalSize == bestSelectivity){
                    // do not need to check whether optimalIndex != null since totalSize
                    // can never be equals to FLOAT.MAX_VALUE
                    optimalIndex = optimalIndex.getType() == IndexTypeEnum.TREE ? secIndex : optimalIndex;
                    continue;
                }

                optimalIndex = secIndex;
                bestSelectivity = totalSize;

            }

        }

        // no index was chosen
        if(optimalIndex != null){
            return optimalIndex;
        } else {
            // bestSelectivity = table.size();
            if(!table.hasPrimaryKey()){
                // we still need to return a list...
                return table.getInternalIndex();
            } else {
                return table.getPrimaryIndex();
            }
        }

        // other alternative is instead of getting all columns are checking against the indexes
        // I can get all indexes and the columns not part of any index are removed from the search

    }

    private List<int[]> getCombinationsFor2SizeColumnList( final int[] filterColumns ){
        int[] arr0 = { filterColumns[0] };
        int[] arr1 = { filterColumns[1] };
        int[] arr2 = { filterColumns[0], filterColumns[1] };
        List<int[]> res = new ArrayList<>();
        res.add(arr0);
        res.add(arr1);
        res.add(arr2);
        return res;
    }

    // TODO later get metadata to know whether such a column has an index
    // TODO a column can appear more than once. make it sure it appears only once
    public List<int[]> getAllPossibleColumnCombinations( final int[] filterColumns ){

        // TODO for one and three values I can do it directly without using an n^2 algorithm
        if(filterColumns.length == 2) return getCombinationsFor2SizeColumnList(filterColumns);

        final int length = filterColumns.length;

        // not exact number, an approximation!
        int totalComb = (int) Math.pow( length, 2 );

        List<int[]> listRef = new ArrayList<>( totalComb );

        for(int i = 0; i < length - 1; i++){
            int[] base = Arrays.copyOfRange( filterColumns, i, i+1 );
            listRef.add ( base );
            for(int j = i+1; j < length; j++ ) {
                listRef.add(Arrays.copyOfRange(filterColumns, i, j + 1));

                // now get all possibilities without this j
                if (j < length - 1) {
                    int k = j + 1;
                    int[] aux1 = {filterColumns[i], filterColumns[k]};
                    listRef.add(aux1);

                    // if there are more elements, then I form a new array including my i and k
                    // i.e., is k the last element? if not, then I have to perform the next operation
                    if (k < length - 1) {
                        int[] aux2 = Arrays.copyOfRange(filterColumns, k, length);
                        int[] aux3 = Arrays.copyOf(base, base.length + aux2.length);
                        System.arraycopy(aux2, 0, aux3, base.length, aux2.length);
                        listRef.add(aux3);
                    }

                }

            }

        }

        listRef.add( Arrays.copyOfRange( filterColumns, length - 1, length ) );

        return listRef;

    }



}
