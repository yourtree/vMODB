package dk.ku.di.dms.vms.database.query.planner.node.join;

import dk.ku.di.dms.vms.database.query.planner.node.filter.IFilter;

/**
 * A data class to encapsulate the join-related information
 * The information is used to reason about the condition of a join operation
 */
public final class JoinCondition {

    /** The actual predicate */
    public final IFilter filter;

    /** The columns to probe */
    public final int columnLeft;

    public final int columnRight;

    public JoinCondition(IFilter filter, int columnLeft, int columnRight) {
        this.filter = filter;
        this.columnLeft = columnLeft;
        this.columnRight = columnRight;
    }

}
