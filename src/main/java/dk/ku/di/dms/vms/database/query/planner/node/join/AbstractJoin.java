package dk.ku.di.dms.vms.database.query.planner.node.join;

import dk.ku.di.dms.vms.database.query.planner.node.filter.FilterInfo;
import dk.ku.di.dms.vms.database.store.index.AbstractIndex;
import dk.ku.di.dms.vms.database.store.row.IKey;

/**
 * Interface to simplify the grouping of join operators in the planner
 * A class implementing IJoin simply means a type of JOIN operator
 */
public abstract class AbstractJoin {

    protected final AbstractIndex<IKey> innerIndex;
    protected final AbstractIndex<IKey> outerIndex;

    protected FilterInfo filterInner;
    protected FilterInfo filterOuter;

    public AbstractJoin(AbstractIndex<IKey> innerIndex, AbstractIndex<IKey> outerIndex) {
        this.innerIndex = innerIndex;
        this.outerIndex = outerIndex;
    }

    public abstract JoinTypeEnum getType();

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

}
