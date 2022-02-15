package dk.ku.di.dms.vms.database.query.planner.node.join;

import dk.ku.di.dms.vms.database.query.planner.node.filter.IFilter;
import dk.ku.di.dms.vms.database.query.planner.utils.IdentifiableNode;

import java.util.Collection;

/**
 * A data class to encapsulate the join-related objects to apply to a given join operation
 */
public final class FilterJoinInfo {

    /** The actual predicate */
    public final IFilter filter;

    /** The columns to probe */
    public final int columnLeft;

    public final int columnRight;

    public FilterJoinInfo(IFilter filter, int columnLeft, int columnRight) {
        this.filter = filter;
        this.columnLeft = columnLeft;
        this.columnRight = columnRight;
    }

}
