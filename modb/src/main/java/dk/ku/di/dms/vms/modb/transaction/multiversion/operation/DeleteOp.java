package dk.ku.di.dms.vms.modb.transaction.multiversion.operation;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IIndexKey;

public class DeleteOp extends DataItemVersion {

    protected DeleteOp(long tid, IIndexKey indexKey, IKey pk) {
        super(tid, indexKey, pk);
    }

}
