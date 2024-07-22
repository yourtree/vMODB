package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

public final class TransactionContext {

    private static final Deque<Set<IKey>> WRITE_SET_BUFFER = new ConcurrentLinkedDeque<>();

    private static final Deque<Set<IMultiVersionIndex>> INDEX_SET_BUFFER = new ConcurrentLinkedDeque<>();

    public final long tid;

    public final long lastTid;

    public final boolean readOnly;

    public final Set<IMultiVersionIndex> indexes;

    // write set of primary index
    public final Set<IKey> writeSet;

    public TransactionContext(long tid, long lastTid, boolean readOnly) {
        this.tid = tid;
        this.lastTid = lastTid;
        this.readOnly = readOnly;
        if(!readOnly) {
            // 4 maximum indexes used per task
            this.indexes = Objects.requireNonNullElseGet(INDEX_SET_BUFFER.poll(), HashSet::new);
            this.writeSet = Objects.requireNonNullElseGet(WRITE_SET_BUFFER.poll(), HashSet::new);
        } else {
            this.indexes = Collections.emptySet();
            this.writeSet = Collections.emptySet();
        }
    }

    public void close(){
        this.writeSet.clear();
        this.indexes.clear();
        WRITE_SET_BUFFER.addLast(this.writeSet);
        INDEX_SET_BUFFER.addLast(this.indexes);
    }

}
