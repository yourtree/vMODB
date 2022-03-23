package dk.ku.di.dms.vms.database.query.planner.operator.result;

import dk.ku.di.dms.vms.database.query.planner.operator.result.interfaces.IOperatorResult;
import dk.ku.di.dms.vms.infra.AbstractEntity;
import java.util.List;

public class EntityOperatorResult implements IOperatorResult {

    private final List<? extends AbstractEntity<?>> entities;

    public EntityOperatorResult(final List<? extends AbstractEntity<?>> entities){
        this.entities = entities;
    }

    @Override
    public EntityOperatorResult asEntityOperatorResult(){
        return this;
    }

    public List<? extends AbstractEntity<?>> getEntities(){
        return this.entities;
    }

}
