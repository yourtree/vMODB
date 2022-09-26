package dk.ku.di.dms.vms.modb.transaction.multiversion.operation;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IIndexKey;

public class InsertOp extends DataItemVersion {

    // to copy the values to the query result
    public long bufferAddress;

    protected InsertOp(long tid, long bufferAddress, IIndexKey indexKey, IKey pk) {
        super(tid, indexKey, pk);
        this.bufferAddress = bufferAddress;
    }

}
