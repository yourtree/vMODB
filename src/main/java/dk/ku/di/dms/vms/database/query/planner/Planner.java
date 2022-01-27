package dk.ku.di.dms.vms.database.query.planner;

import dk.ku.di.dms.vms.database.query.analyzer.QueryTree;

public class Planner {

    // receive a statement

    public PlanTree plan(final QueryTree queryTree){

        // define the steps, indexes to use, do we need to sort, limit, group?....
        // define the rules to apply based on the set of pre-defined rules

        // join rule always prune the shorter relation...
        // always apply the filter first

        // TODO FINISH

        return null;
    }

}
