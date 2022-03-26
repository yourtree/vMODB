package dk.ku.di.dms.vms.modb.query.planner.operator.result;

import dk.ku.di.dms.vms.modb.common.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.query.planner.operator.result.interfaces.IOperatorResult;
import java.util.List;

public class EntityOperatorResult implements IOperatorResult {

    private final List<? extends IEntity<?>> entities;

    public EntityOperatorResult(final List<? extends IEntity<?>> entities){
        this.entities = entities;
    }

    @Override
    public EntityOperatorResult asEntityOperatorResult(){
        return this;
    }

    public List<? extends IEntity<?>> getEntities(){
        return this.entities;
    }

}
