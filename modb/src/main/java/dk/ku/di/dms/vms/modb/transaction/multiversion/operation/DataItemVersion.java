package dk.ku.di.dms.vms.modb.transaction.multiversion.operation;

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

    protected DataItemVersion(long tid) {
        this.tid = tid;
    }

    public long tid(){
        return this.tid;
    }

    public static DataItemVersion update(long tid, int columnIndex, long address){
        return new UpdateOp(tid, columnIndex, address);
    }

    public static DataItemVersion insert(long tid, long bufferAddress){
        return new InsertOp(tid, bufferAddress);
    }

    public static DataItemVersion delete(long tid){
        return new DeleteOp(tid);
    }

}
