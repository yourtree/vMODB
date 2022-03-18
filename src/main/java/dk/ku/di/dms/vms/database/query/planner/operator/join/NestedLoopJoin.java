package dk.ku.di.dms.vms.database.query.planner.operator.join;

import dk.ku.di.dms.vms.database.query.planner.operator.OperatorResult;
import dk.ku.di.dms.vms.database.store.index.AbstractIndex;
import dk.ku.di.dms.vms.database.store.common.IKey;

public class NestedLoopJoin extends AbstractJoin {

    // private JoinCondition filterJoin;

    public NestedLoopJoin(int identifier, AbstractIndex<IKey> innerIndex, AbstractIndex<IKey> outerIndex) {
        super(identifier, innerIndex, outerIndex);
    }

    @Override
    public JoinTypeEnum getType() {
        return JoinTypeEnum.NESTED_LOOP;
    }

    @Override
    public OperatorResult get() {
        // TODO finish
        return null;
    }
}
