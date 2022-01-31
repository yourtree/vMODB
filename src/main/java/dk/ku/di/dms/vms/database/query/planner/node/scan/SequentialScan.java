package dk.ku.di.dms.vms.database.query.planner.node.scan;

import dk.ku.di.dms.vms.database.query.planner.node.filter.IFilter;

import java.util.List;

public final class SequentialScan {

    private final List<IFilter<?>> filters;

    public SequentialScan(final List<IFilter<?>> filters) {
        if (filters.size() < 1) {
            this.filters = null;
            return;
        }
        this.filters = filters;
    }

    public SequentialScan(){
        this.filters = null;
    }

    // TODO iterate?

}
