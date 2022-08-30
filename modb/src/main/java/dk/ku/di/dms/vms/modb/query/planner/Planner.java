package dk.ku.di.dms.vms.modb.query.planner;

import dk.ku.di.dms.vms.modb.query.analyzer.QueryTree;

/**
 * Planner that only takes into consideration simple queries.
 * Those involving single tables and filters, and no
 * aggregation or joins.
 */
public class Planner {

    public AbstractOperator plan(QueryTree queryTree) {


        return null;

    }

}