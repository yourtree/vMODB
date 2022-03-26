package dk.ku.di.dms.vms.modb.query.planner.operator.join;

import dk.ku.di.dms.vms.modb.query.planner.operator.result.RowOperatorResult;
import dk.ku.di.dms.vms.modb.store.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.store.common.IKey;

public class NestedLoopJoin extends AbstractJoin {

    // private JoinCondition filterJoin;

    public NestedLoopJoin(int identifier, AbstractIndex<IKey> innerIndex, AbstractIndex<IKey> outerIndex) {
        super(identifier, innerIndex, outerIndex);
    }

    @Override
    public JoinOperatorTypeEnum getType() {
        return JoinOperatorTypeEnum.NESTED_LOOP;
    }

    @Override
    public RowOperatorResult get() {
        // TODO finish
        return null;
    }
}
