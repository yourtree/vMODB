package dk.ku.di.dms.vms.modb.transaction.multiversion;

import dk.ku.di.dms.vms.modb.transaction.multiversion.operation.DeleteOp;
import dk.ku.di.dms.vms.modb.transaction.multiversion.operation.InsertOp;
import dk.ku.di.dms.vms.modb.transaction.multiversion.operation.UpdateOp;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The set of operations applied to a given index key
 */
public class OperationSet {

    public DeleteOp deleteOp;

    public InsertOp insertOp;

    // keyed by columnIndex and ordered by TID (increasing)
    public Map<Integer, List<UpdateOp>> updateOps;

    public OperationSet(){
        this.updateOps = new HashMap<>(10);
    }

}
