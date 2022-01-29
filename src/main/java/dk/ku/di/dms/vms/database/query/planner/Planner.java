package dk.ku.di.dms.vms.database.query.planner;

import dk.ku.di.dms.vms.database.query.analyzer.QueryTree;

public class Planner {

    // https://www.interdb.jp/pg/pgsql03.html
    // "The planner receives a query tree from the rewriter and generates a (query)
    // plan tree that can be processed by the executor most effectively."


    public PlanTree plan(final QueryTree queryTree){

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
        // many layers orm introduces, codification and decodification
        // of application-defined objects into the underlying store. pays a performance price
        // at the same time developers challenge with caching mechanisms. that should be handled natively
        // by a MMDBMS



        // TODO FINISH

        return null;
    }

    // naively defining a plan tree, without considering the cost involved
    // 1- filter, 2- join, 3 - selection
    private PlanTree naive(final QueryTree queryTree){

        return null;

    }

}
