package dk.ku.di.dms.vms.modb.transaction.multiversion;

import dk.ku.di.dms.vms.modb.transaction.multiversion.operation.DeleteOp;
import dk.ku.di.dms.vms.modb.transaction.multiversion.operation.InsertOp;
import dk.ku.di.dms.vms.modb.transaction.multiversion.operation.UpdateOp;

import java.util.ArrayList;
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
    // ideally must be indexed decreasingly
    public Map<Integer, List<UpdateOp>> columnUpdateOps;
    public List<UpdateOp> recordUpdateOps;

    public enum Type {
        DELETE,
        UPDATE,
        INSERT
    }

    public Type lastWriteType;

    /**
     * It is tradeoff of easier explanability of the code and fine-tracking of updates
     * Sometimes there are more than one column updated, that means the columnUpdateOps
     * will have two entries. That increases over time...
     */
    public OperationSet(){
        this.columnUpdateOps = new HashMap<>(2);
        this.recordUpdateOps = new ArrayList<>(2);
    }

}
