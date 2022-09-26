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
    protected long tid;

    protected IKey pk;

    protected IIndexKey indexKey;

    protected DataItemVersion(long tid) {
        this.tid = tid;
    }

    protected DataItemVersion(long tid, IIndexKey indexKey, IKey pk) {
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

    public static UpdateOp update(long tid, int columnIndex, long address){
        return new UpdateOp(tid, columnIndex, address);
    }

    public static InsertOp insert(long tid, long bufferAddress){
        return new InsertOp(tid, bufferAddress);
    }

    public static DeleteOp delete(long tid){
        return new DeleteOp(tid);
    }

}
