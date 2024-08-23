package dk.ku.di.dms.vms.modb.common.transaction;

import java.io.Closeable;

public abstract class TransactionContextBase implements Closeable {

    public final long tid;

    public final long lastTid;

    public final boolean readOnly;

    public TransactionContextBase(long tid, long lastTid, boolean readOnly) {
        this.tid = tid;
        this.lastTid = lastTid;
        this.readOnly = readOnly;
    }

}
