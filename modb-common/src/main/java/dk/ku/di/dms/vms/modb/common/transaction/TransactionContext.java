package dk.ku.di.dms.vms.modb.common.transaction;

import dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum;

public class TransactionContext {

    public TransactionId tid;

    public TransactionId lastTid;

    public TransactionTypeEnum type;

    public TransactionContext(TransactionId tid, TransactionId lastTid, TransactionTypeEnum type) {
        this.tid = tid;
        this.lastTid = lastTid;
        this.type = type;
    }

}
