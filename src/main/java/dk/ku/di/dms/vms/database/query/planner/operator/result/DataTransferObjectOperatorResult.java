package dk.ku.di.dms.vms.database.query.planner.operator.result;

import dk.ku.di.dms.vms.database.query.planner.operator.result.interfaces.IReturnableOperatorResult;
import java.util.List;

public class DataTransferObjectOperatorResult implements IReturnableOperatorResult<List<Object>> {

    // As we are building the DTOs, we can opt for list
    // Maybe all DTOs must have the same inheritance, e.g., AbstractDTO
    private List<Object> dataTransferObjects;

    public DataTransferObjectOperatorResult(final List<Object> dataTransferObjects){
        this.dataTransferObjects = dataTransferObjects;
    }


    public List<Object> getDataTransferObjects() {
        return dataTransferObjects;
    }


    @Override
    public void accept(List<Object> objects) {

    }
}
