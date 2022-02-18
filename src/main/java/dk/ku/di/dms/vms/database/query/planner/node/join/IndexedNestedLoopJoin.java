package dk.ku.di.dms.vms.database.query.planner.node.join;

import dk.ku.di.dms.vms.database.query.planner.OperatorResult;
import dk.ku.di.dms.vms.database.query.planner.node.filter.FilterInfo;
import dk.ku.di.dms.vms.database.store.index.AbstractIndex;
import dk.ku.di.dms.vms.database.store.row.IKey;

import java.util.function.Supplier;

public class IndexedNestedLoopJoin implements Supplier<OperatorResult>, IJoin {

    private final AbstractIndex<IKey> innerIndex;
    private final AbstractIndex<IKey> outerIndex;

    private FilterInfo filterInner;
    private FilterInfo filterOuter;

    public IndexedNestedLoopJoin(final AbstractIndex<IKey> innerIndex,
                                 final AbstractIndex<IKey> outerIndex) {
        this.innerIndex = innerIndex;
        this.outerIndex = outerIndex;
    }

    @Override
    public OperatorResult get() {
        // TODO finish
        return null;
    }

}
