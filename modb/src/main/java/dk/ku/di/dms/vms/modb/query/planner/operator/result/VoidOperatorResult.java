package dk.ku.di.dms.vms.modb.query.planner.operator.result;

import dk.ku.di.dms.vms.modb.query.planner.operator.result.interfaces.IOperatorResult;
import dk.ku.di.dms.vms.modb.query.planner.operator.result.meta.Status;

/**
 * To reflect operations that return no value, such as updates and inserts.
 */
public class VoidOperatorResult implements IOperatorResult {

    private final Status status;

    public VoidOperatorResult(Status status) {
        this.status = status;
    }
}
