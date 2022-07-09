package dk.ku.di.dms.vms.modb.query.planner.operator.join;

import dk.ku.di.dms.vms.modb.query.planner.operator.result.RowOperatorResult;
import dk.ku.di.dms.vms.modb.schema.key.IKey;
import dk.ku.di.dms.vms.modb.index.onheap.AbstractIndex;

public class IndexedNestedLoopJoin extends AbstractJoin {

    public IndexedNestedLoopJoin(int identifier, AbstractIndex<IKey> innerIndex, AbstractIndex<IKey> outerIndex) {
        super(identifier, innerIndex, outerIndex);
    }

    @Override
    public JoinOperatorTypeEnum getType() {
        return JoinOperatorTypeEnum.INDEX_NESTED_LOOP;
    }

    @Override
    public RowOperatorResult get() {
        // TODO finish
        return null;
    }

}
