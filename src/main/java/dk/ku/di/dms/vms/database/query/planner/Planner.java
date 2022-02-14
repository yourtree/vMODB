package dk.ku.di.dms.vms.database.query.planner;

import dk.ku.di.dms.vms.database.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.database.query.analyzer.predicate.JoinPredicate;
import dk.ku.di.dms.vms.database.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.database.query.parser.enums.ExpressionEnum;
import dk.ku.di.dms.vms.database.query.planner.node.filter.IFilter;
import dk.ku.di.dms.vms.database.query.planner.node.filter.FilterBuilder;
import dk.ku.di.dms.vms.database.query.planner.node.scan.SequentialScan;
import dk.ku.di.dms.vms.database.query.planner.utils.IdentifiableNode;
import dk.ku.di.dms.vms.database.store.table.Table;
import jdk.nashorn.internal.objects.annotations.Where;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Supplier;
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


        for(final JoinPredicate joinPredicate : queryTree.joinPredicates){

            // get filters for this join

            Table tableLeft = joinPredicate.getLeftTable();

            // first the left table
            // List<IFilter<?>>

            // TODO can we merge the filters with the join? i think so
            // remove from map after applying the additional filters
//            filtersForJoinGroupedByTable

            // TODO the shortest the table, lower the degree of the join operation in the query tree

        }

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

            int index = 0;
            for(final WherePredicate w : wherePredicates){

                boolean nullable = w.expression == ExpressionEnum.IS_NOT_NULL || w.expression == ExpressionEnum.IS_NULL;

                filters[ index ] = FilterBuilder.build( w );
                filterColumns[ index ] = w.columnReference.columnIndex;
                if( !nullable ){
                    filterParams.add( new IdentifiableNode<>( index, w.value ) );
                }

                index++;
            }

            // TODO is there any index that can help? from all the indexes, which one gives the best selectivity?

            final SequentialScan seqScan = new SequentialScan( currTable, filters, filterColumns, filterParams );


            // Executor exec = Executors.newScheduledThreadPool()

            Supplier<OperatorResult> sup = new Supplier<OperatorResult>() {

                @Override
                public OperatorResult get() {
                    return null;
                }
            };

            Consumer<OperatorResult> con = new Consumer<OperatorResult>() {
                @Override
                public void accept(OperatorResult operatorResult) {

                }
            };


        }


        // TODO think about parallel seq scan
        // SequentialScan sequentialScan = new SequentialScan( filterList, table);

        return null;
    }

}
