package dk.ku.di.dms.vms.modb.transaction;

import java.util.Objects;

public class TransactionId {

    private final long tid;

    private final long bid;

    private final int hash;

    public TransactionId(long tid, long bid) {
        this.tid = tid;
        this.bid = bid;
        this.hash = Objects.hash(tid, bid);
    }

    @Override
    public boolean equals(Object o) {
        return o.hashCode() == this.hash;
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
