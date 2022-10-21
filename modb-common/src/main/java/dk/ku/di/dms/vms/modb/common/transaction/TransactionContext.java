package dk.ku.di.dms.vms.modb.common.transaction;

public class TransactionContext {

    public TransactionId tid;

    public TransactionId lastTid;

    public boolean readOnly;

    public TransactionContext(TransactionId tid, TransactionId lastTid, boolean readOnly) {
        this.tid = tid;
        this.lastTid = lastTid;
        this.readOnly = readOnly;
    }

}
