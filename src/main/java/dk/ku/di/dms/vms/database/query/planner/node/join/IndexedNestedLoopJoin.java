package dk.ku.di.dms.vms.database.query.planner.node.join;

import dk.ku.di.dms.vms.database.query.planner.OperatorResult;
import dk.ku.di.dms.vms.database.store.index.AbstractIndex;
import dk.ku.di.dms.vms.database.store.common.IKey;

public class IndexedNestedLoopJoin extends AbstractJoin {

    public IndexedNestedLoopJoin(int identifier, AbstractIndex<IKey> innerIndex, AbstractIndex<IKey> outerIndex) {
        super(identifier, innerIndex, outerIndex);
    }

    @Override
    public JoinTypeEnum getType() {
        return JoinTypeEnum.INDEX_NESTED_LOOP;
    }

    @Override
    public OperatorResult get() {
        // TODO finish
        return null;
    }

}
