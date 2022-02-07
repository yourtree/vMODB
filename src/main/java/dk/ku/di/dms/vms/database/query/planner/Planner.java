package dk.ku.di.dms.vms.database.query.planner;

import dk.ku.di.dms.vms.database.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.database.query.analyzer.predicate.JoinPredicate;
import dk.ku.di.dms.vms.database.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.database.query.planner.node.filter.IFilter;
import dk.ku.di.dms.vms.database.query.planner.node.filter.FilterBuilder;
import dk.ku.di.dms.vms.database.query.planner.node.scan.SequentialScan;
import dk.ku.di.dms.vms.database.store.Table;
import dk.ku.di.dms.vms.database.store.refac.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class Planner {

    // https://www.interdb.jp/pg/pgsql03.html
    // "The planner receives a query tree from the rewriter and generates a (query)
    // plan tree that can be processed by the executor most effectively."

    // plan and optimize in the same step
    public PlanTree plan(final QueryTree queryTree) throws Exception {

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

        Map<Table<?,?>,List<WherePredicate>> whereClauseGroupedByTable =
                queryTree
                        .wherePredicates.stream()
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


        // TODO FINISH

        // TODO are we automatically creating indexes for foreign key? probably not


        for( Map.Entry<Table<?,?>, List<WherePredicate>> entry : whereClauseGroupedByTable.entrySet() ){

            Table<?,?> currTable = entry.getKey();
            // the row is true at the start anyway
            IFilter<Row> baseFilter = ( row -> true );
            for(WherePredicate whereClause : entry.getValue()){
                baseFilter.and(FilterBuilder.build( whereClause ));
            }

            // a sequential scan for each table

            // TODO think about parallel seq scan
            // SequentialScan sequentialScan = new SequentialScan( filterList, table);

        }

        // TODO can we merge the filters with the join?
        for(JoinPredicate joinClause : queryTree.joinOperations){

            // joinClause.

        }


        return null;
    }

}
