package dk.ku.di.dms.vms.modb.common.transaction;

public final class TransactionContext {

    public final long tid;

    public final long lastTid;

    public final boolean readOnly;

    public TransactionContext(long tid, long lastTid, boolean readOnly) {
        this.tid = tid;
        this.lastTid = lastTid;
        this.readOnly = readOnly;
    }

}
