package dk.ku.di.dms.vms.database.query.planner.node.join;

import dk.ku.di.dms.vms.database.query.planner.OperatorResult;
import dk.ku.di.dms.vms.database.store.index.AbstractIndex;
import dk.ku.di.dms.vms.database.store.row.IKey;

public class NestedLoopJoin extends AbstractJoin {

    // private JoinCondition filterJoin;

    public NestedLoopJoin(AbstractIndex<IKey> innerIndex, AbstractIndex<IKey> outerIndex) {
        super(innerIndex, outerIndex);
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
