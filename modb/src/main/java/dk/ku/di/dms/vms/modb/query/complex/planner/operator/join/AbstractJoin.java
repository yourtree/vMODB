package dk.ku.di.dms.vms.modb.query.complex.planner.operator.join;

import dk.ku.di.dms.vms.modb.query.refac.FilterContext;
import dk.ku.di.dms.vms.modb.query.complex.planner.operator.AbstractOperator;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;

/**
 * Interface to simplify the grouping of join operators in the planner.
 */
public abstract class AbstractJoin extends AbstractOperator {

    // outer, aka the smaller
    protected final AbstractIndex<IKey> leftIndex;

    // inner
    protected final AbstractIndex<IKey> rightIndex;

    protected final FilterContext leftFilter;
    protected final FilterContext rightFilter;


    public AbstractJoin(int id, int entrySize,
                        AbstractIndex<IKey> outerIndex,
                        AbstractIndex<IKey> innerIndex,
                        FilterContext filterInner, FilterContext filterOuter){
        super(id, entrySize);
        this.leftIndex = outerIndex;
        this.rightIndex = innerIndex;
        this.leftFilter = filterOuter;
        this.rightFilter = filterInner;
    }

    protected void append(long address1, long address2) {
        ensureMemoryCapacity();
        this.currentBuffer.append(address1);
        this.currentBuffer.append(address2);
    }

    public abstract JoinOperatorTypeEnum getType();

}
