package dk.ku.di.dms.vms.modb.transaction.multiversion.operation;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IIndexKey;

/**
 * Contains:
 * - the transaction that have been modified a data item
 * - the index
 * - the key (in the index)
 * - the change/value (after-write)
 *
 * Should be a facade, a interface? so references are not stored in memory for nothing..?
 *
 */
public abstract class DataItemVersion {

    // transaction id that modified the item
    protected final long tid;

    protected final IKey pk;

    protected final IIndexKey indexKey;

    public DataItemVersion(long tid, IIndexKey indexKey, IKey pk) {
        this.tid = tid;
        this.indexKey = indexKey;
        this.pk = pk;
    }

    public long tid(){
        return this.tid;
    }

    public IKey pk() {
        return this.pk;
    }

    public IIndexKey indexKey() {
        return this.indexKey;
    }

    public static UpdateOp update(long tid, int columnIndex, long address, IIndexKey indexKey, IKey pk){
        return new UpdateOp(tid, columnIndex, address, indexKey, pk);
    }

    public static UpdateOp update(long tid, long address, IIndexKey indexKey, IKey pk){
        return new UpdateOp(tid, address, indexKey, pk);
    }

    public static InsertOp insert(long tid, long bufferAddress, IIndexKey indexKey, IKey pk){
        return new InsertOp(tid, bufferAddress, indexKey, pk);
    }

    public static DeleteOp delete(long tid, IIndexKey indexKey, IKey pk){
        return new DeleteOp(tid, indexKey, pk);
    }

}
