package dk.ku.di.dms.vms.modb.query.planner.operator.result.interfaces;

import dk.ku.di.dms.vms.modb.query.planner.operator.result.EntityOperatorResult;
import dk.ku.di.dms.vms.modb.query.planner.operator.result.DataTransferObjectOperatorResult;
import dk.ku.di.dms.vms.modb.query.planner.operator.result.RowOperatorResult;
import dk.ku.di.dms.vms.modb.query.planner.operator.scan.IndexScan;

/**
 * This interface is used in classes agnostic to operator implementation, e.g., { SequentialQueryExecutor}
 * Low-level operations like {@link IndexScan} use the concrete implementations, like {@link RowOperatorResult}
 */
public interface IOperatorResult {

    default EntityOperatorResult asEntityOperatorResult(){
        return null;
    }

    default DataTransferObjectOperatorResult asDataTransferObjectOperatorResult(){
        return null;
    }

    default RowOperatorResult asRowOperatorResult(){
        return null;
    }

}
