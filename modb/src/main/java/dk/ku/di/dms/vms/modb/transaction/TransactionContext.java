package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

public final class TransactionContext {

    private static final Deque<Set<IMultiVersionIndex>> INDEX_SET_BUFFER = new ConcurrentLinkedDeque<>();

    public final long tid;

    public final long lastTid;

    public final boolean readOnly;

    public final Set<IMultiVersionIndex> indexes;

    public TransactionContext(long tid, long lastTid, boolean readOnly) {
        this.tid = tid;
        this.lastTid = lastTid;
        this.readOnly = readOnly;
        if(!readOnly) {
            // 4 maximum indexes used per task
            this.indexes = Objects.requireNonNullElseGet(INDEX_SET_BUFFER.poll(), HashSet::new);
        } else {
            this.indexes = Collections.emptySet();
        }
    }

    public void close(){
        this.indexes.clear();
        INDEX_SET_BUFFER.addLast(this.indexes);
    }

}
