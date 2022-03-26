package dk.ku.di.dms.vms.modb.query.planner.operator.join;

import dk.ku.di.dms.vms.modb.query.planner.operator.filter.FilterInfo;
import dk.ku.di.dms.vms.modb.query.planner.operator.result.interfaces.IOperatorResult;
import dk.ku.di.dms.vms.modb.store.common.IKey;
import dk.ku.di.dms.vms.modb.store.index.AbstractIndex;

import java.util.function.Supplier;

/**
 * Interface to simplify the grouping of join operators in the planner
 * A class implementing IJoin simply means a type of JOIN operator.
 * All types of join operators supply a result.
 */
public abstract class AbstractJoin implements Supplier<IOperatorResult> {

    // used to uniquely identify this join operation in a query plan
    private final int identifier;

    protected final AbstractIndex<IKey> innerIndex;
    protected final AbstractIndex<IKey> outerIndex;

    protected FilterInfo filterInner;
    protected FilterInfo filterOuter;

    public AbstractJoin(final int identifier, AbstractIndex<IKey> innerIndex, AbstractIndex<IKey> outerIndex) {
        this.identifier = identifier;
        this.innerIndex = innerIndex;
        this.outerIndex = outerIndex;
    }

    public abstract JoinOperatorTypeEnum getType();

    public void setFilterInner(final FilterInfo filterInner) {
        this.filterInner = filterInner;
    }

    public void setFilterOuter(final FilterInfo filterOuter) {
        this.filterOuter = filterOuter;
    }

    public AbstractIndex<IKey> getInnerIndex(){
        return innerIndex;
    }

    public AbstractIndex<IKey> getOuterIndex(){
        return outerIndex;
    }

    public int getIdentifier() {
        return identifier;
    }
}
