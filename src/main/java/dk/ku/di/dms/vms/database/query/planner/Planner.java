package dk.ku.di.dms.vms.database.query.planner;

import dk.ku.di.dms.vms.database.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.database.query.analyzer.predicate.JoinPredicate;
import dk.ku.di.dms.vms.database.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.database.query.planner.node.filter.FilterInfo;
import dk.ku.di.dms.vms.database.query.planner.node.filter.IFilter;
import dk.ku.di.dms.vms.database.query.planner.node.filter.FilterBuilder;
import dk.ku.di.dms.vms.database.query.planner.node.join.*;
import dk.ku.di.dms.vms.database.query.planner.node.scan.SequentialScan;
import dk.ku.di.dms.vms.database.query.planner.utils.IdentifiableNode;
import dk.ku.di.dms.vms.database.store.index.AbstractIndex;
import dk.ku.di.dms.vms.database.store.index.HashIndex;
import dk.ku.di.dms.vms.database.store.index.IndexDataStructureEnum;
import dk.ku.di.dms.vms.database.store.row.IKey;
import dk.ku.di.dms.vms.database.store.row.SimpleKey;
import dk.ku.di.dms.vms.database.store.table.Table;

import java.util.*;
import java.util.stream.Collectors;

import static dk.ku.di.dms.vms.database.query.planner.node.join.JoinTypeEnum.*;

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

        Map<Table,List<AbstractJoin>> joinsPerTable = new HashMap<>();

        // TODO I am not considering a JOIN predicate may contain more than a column....
        // if the pk or secIndex only applies to one of the columns, then others become filters
        for(final JoinPredicate joinPredicate : queryTree.joinPredicates){

            boolean leftTableIsIndexed = false;
            AbstractIndex<IKey> leftTableIndex = null;

            Table tableLeft = joinPredicate.getLeftTable();

            // this approach used for composite key
            //int[] colIdxArr = { joinPredicate.columnLeftReference.columnIndex };
            //int colHash = Arrays.hashCode(colIdxArr);

            // approach used for simple key
            SimpleKey keyLeft = new SimpleKey( joinPredicate.columnLeftReference.columnIndex );

            // if there is a join condition based on primary key then I can bypass the index selection step
            // of course there may the case where the developer can define a tree-base pk and hash-based index
            // but in our case we can constrain that through our annotation-based semantics...
            if(tableLeft.hasPrimaryKey()){
                int pIndexHash = tableLeft.getPrimaryIndex().hashCode();
                if (pIndexHash == keyLeft.hashCode()){
                    leftTableIndex = tableLeft.getPrimaryIndex();
                    leftTableIsIndexed = true;
                }
            }

            if( !leftTableIsIndexed && tableLeft.getSecondaryIndexForColumnSetHash(keyLeft) != null ){
                leftTableIndex = tableLeft.getSecondaryIndexForColumnSetHash(keyLeft);
                leftTableIsIndexed = true;

            //else if(tableLeft.getSecondaryIndexes().size() > 0){
                // then only consider the filters for this table
                // maybe I should add the filters after I build all the join operators...
                // exactly... the filters may even change the operator??? because the filter may have a better index...
                // the question is: do I have an index to apply on a filter that is even better than the one chosen?

            } //else {
                // in this case the hope is finding an index for a filter...
            //}

            boolean rightTableIsIndexed = false;
            AbstractIndex<IKey> rightTableIndex = null;

            Table tableRight = joinPredicate.getRightTable();

            SimpleKey keyRight = new SimpleKey( joinPredicate.columnRightReference.columnIndex );

            if(tableRight.hasPrimaryKey()){
                int pIndexHash = tableRight.getPrimaryIndex().hashCode();
                if (pIndexHash == keyRight.hashCode()){
                    rightTableIndex = tableRight.getPrimaryIndex();
                    rightTableIsIndexed = true;
                }
            }

            if( !rightTableIsIndexed && tableRight.getSecondaryIndexForColumnSetHash(keyLeft) != null ) {
                rightTableIndex = tableRight.getSecondaryIndexForColumnSetHash(keyLeft);
                rightTableIsIndexed = true;
            }

            final AbstractJoin join;
            final Table tableRef;

            // now we have to decide the operators
            if(leftTableIsIndexed && rightTableIsIndexed){
                // does this case necessarily lead to symmetric HashJoin? https://cs.uwaterloo.ca/~david/cs448/

                if(leftTableIndex.size() > rightTableIndex.size()){
                    join = new HashJoin( rightTableIndex, leftTableIndex );
                    tableRef = tableRight;
                } else {
                    join = new HashJoin( leftTableIndex, rightTableIndex );
                    tableRef = tableLeft;
                }

            } else if( leftTableIsIndexed ){ // indexed nested loop join on inner table. the outer probes its rows
                tableRef = tableRight;
                join = new IndexedNestedLoopJoin( tableRight.getInternalIndex(), leftTableIndex );
            } else if( rightTableIsIndexed ) {
                tableRef = tableLeft;
                join = new IndexedNestedLoopJoin( tableLeft.getInternalIndex(), rightTableIndex );
            } else { // nested loop join
                // ideally this is pushed upstream to benefit from pruning performed by downstream nodes
                if(tableLeft.getInternalIndex().size() > tableRight.getInternalIndex().size()){
                    tableRef = tableRight;
                    join = new NestedLoopJoin(tableRight.getInternalIndex(), tableLeft.getInternalIndex());
                } else {
                    tableRef = tableLeft;
                    join = new NestedLoopJoin(tableLeft.getInternalIndex(), tableRight.getInternalIndex());
                }

            }

            List<AbstractJoin> planNodes = joinsPerTable.getOrDefault(tableRef,new ArrayList<>());
            this.addJoinToRespectiveTableInOrder(join, planNodes);

        }

        // TODO optimization. the same table can appear in several joins.
        //      in this sense, which join + filter should be executed first to give the best prune of records?
        //      yes, but without the selectivity info there is nothing we can do. I can store the selectivity info
        //      on index creation, but for that I need to have tuples already stored...

        // Merging the filters with the join
        // The heuristic now is just to apply the filter as early as possible. later we can revisit
        // remove from map after applying the additional filters
        for( Map.Entry<Table,List<WherePredicate>> tableFilter : filtersForJoinGroupedByTable.entrySet() ){
            // joinsPerTable TODO finish it
        }


        // here I need to iterate over the plan nodes defined earlier

        // a scan for each table
        // the scan strategy only applies for those tables not involved in any join
        // TODO all predicates with the same column should be a single filter
        // TODO build filter checks where the number of parameters are known a priori and can
        //      take better advantage of the runtime, i.e., avoid loops over the filters
        //      E.g., eval2(predicate1, values2, predicate2, values2) ...
        //      each one of the tables here lead to a cartesian product operation...
        //      so it should be pushed upstream
        for( final Map.Entry<Table, List<WherePredicate>> entry : whereClauseGroupedByTable.entrySet() ){

            final List<WherePredicate> wherePredicates = entry.getValue();
            final int size = wherePredicates.size();
            IFilter<?>[] filters = new IFilter<?>[size];
            int[] filterColumns = new int[size];
            Collection<IdentifiableNode<Object>> filterParams = new LinkedList<>();

            Table currTable = entry.getKey();

            int i = 0;
            for(final WherePredicate w : wherePredicates){

                boolean nullableExpression = w.expression == ExpressionTypeEnum.IS_NOT_NULL
                        || w.expression == ExpressionTypeEnum.IS_NULL;

                filters[ i ] = FilterBuilder.build( w );
                filterColumns[ i ] = w.columnReference.columnIndex;
                if( !nullableExpression ){
                    filterParams.add( new IdentifiableNode<>( i, w.value ) );
                }

                i++;
            }

            final FilterInfo filterInfo = new FilterInfo(filters, filterColumns, filterParams);
            // I need the index information to decide which operator, e.g., index scan
            final AbstractIndex optimalIndex = findOptimalIndex( currTable, filterInfo.filterColumns, false );
            if(optimalIndex.getType() == IndexDataStructureEnum.HASH){
                // TODO finish new IndexScan()
            } else {
                final SequentialScan seqScan = new SequentialScan(optimalIndex, filterInfo);
            }


        }

        // TODO focus on left deep, most cases will fall in this category

        // TODO think about parallel seq scan,
        // SequentialScan sequentialScan = new SequentialScan( filterList, table);

        return null;
    }

    /** the shortest the table, lower the degree of the join operation in the query tree
     * thus, store a map of plan nodes keyed by table
     * the plan node with the largest probability of pruning (considering index type + filters + size of table)
     * more records earlier in the query are pushed downstream in the physical query plan
     * ordered list of participating tables. the first one is the smallest table and so on.
     * better selectivity rate leads to a deeper node
     * Consider that the base table is always the left one in the join
     * So I just need to check to order the plan nodes according to the size of the right table
     * TODO binary search
     * Priority order of sorting conditions: type of join, selectivity (we are lacking this right now), size of (right) table
     * @param joins
     * @param joins
     */
    private void addJoinToRespectiveTableInOrder( final AbstractJoin join, final List<AbstractJoin> joins){
        if(joins.size() == 0){
            joins.add(join);
            return;
        }
        Iterator<AbstractJoin> it = joins.iterator();
        int posToInsert = 0;
        AbstractJoin next = it.next();
        boolean hasNext = true;
        while (hasNext &&
                (
                  (
                    ( next.getType() == HASH && (join.getType() == INDEX_NESTED_LOOP || join.getType() == NESTED_LOOP ) )
                    || // TODO this may not be true for all joins. e.g., some index nested loop might take longer than a nested loop join
                    ( next.getType() == INDEX_NESTED_LOOP && (join.getType() == NESTED_LOOP ) )
                  )
                  ||
                  ( join.getOuterIndex().size() > next.getOuterIndex().size() )
                )
            )
        {
            if(it.hasNext()){
                next = it.next();
                posToInsert++;
            } else {
                hasNext = false;
            }

        }
        joins.add(posToInsert, join);
    }

    /**
     *  Here we are relying on the fact that the developer has declared the columns
     *  in the where clause matching the actual index column order definition
     */
    private AbstractIndex findOptimalIndex(final Table table, final int[] filterColumns, final boolean skipPrimaryKey){

        // all combinations... TODO this can be built in the previous step...
        final List<int[]> combinations = getAllPossibleColumnCombinations(filterColumns);

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
        if(!skipPrimaryKey && table.hasPrimaryKey()){
            final AbstractIndex pIndex = table.getPrimaryIndex();
            if( hashCodeMap.get( pIndex.hashCode() ) != null ){
                // bestSelectivity = 1D / table.size();
                return pIndex;
            }
        }

        // the selectivity may be unknown, we should use simple heuristic here to decide what is the best index
        // the heuristic is simply the number of columns an index covers
        // of course this can be misleading, since the selectivity can be poor, leading to scan the entire table anyway...
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

                // optimistic belief that the number of columns would lead to a significant pruning of records
                float heuristicSelectivity = (float) columns.length / table.size();

                // try next index then
                if(heuristicSelectivity > bestSelectivity) continue;

                // if tiebreak, hash index type is chosen as the tiebreaker criterion
                if(heuristicSelectivity == bestSelectivity
                        && optimalIndex.getType() == IndexDataStructureEnum.TREE
                        && secIndex.getType() == IndexDataStructureEnum.HASH){
                    // do not need to check whether optimalIndex != null since totalSize
                    // can never be equals to FLOAT.MAX_VALUE
                    optimalIndex = secIndex;
                    continue;
                }

                // heuristicSelectivity < bestSelectivity
                optimalIndex = secIndex;
                bestSelectivity = heuristicSelectivity;

            }

        }

        // no index was chosen
        if(optimalIndex != null){
            return optimalIndex;
        } else {
            // bestSelectivity = table.size();
            if(!table.hasPrimaryKey()){
                // we still need to return a reference to the table rows...
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
        return Arrays.asList( arr0, arr1, arr2 );
    }

    private List<int[]> getCombinationsFor3SizeColumnList( final int[] filterColumns ){
        int[] arr0 = { filterColumns[0] };
        int[] arr1 = { filterColumns[1] };
        int[] arr2 = { filterColumns[2] };
        int[] arr3 = { filterColumns[0], filterColumns[1], filterColumns[2] };
        int[] arr4 = { filterColumns[0], filterColumns[1] };
        int[] arr5 = { filterColumns[0], filterColumns[2] };
        int[] arr6 = { filterColumns[1], filterColumns[2] };
        return Arrays.asList( arr0, arr1, arr2, arr3, arr4, arr5, arr6 );
    }

    // TODO later get metadata to know whether such a column has an index, so it can be pruned from this search
    // TODO a column can appear more than once. make it sure it appears only once
    public List<int[]> getAllPossibleColumnCombinations( final int[] filterColumns ){

        // in case only one condition for join and single filter
        if(filterColumns.length == 1) return Arrays.asList(filterColumns);

        if(filterColumns.length == 2) return getCombinationsFor2SizeColumnList(filterColumns);

        if(filterColumns.length == 3) return getCombinationsFor3SizeColumnList(filterColumns);

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
