package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.common.transaction.ITransactionContext;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Closeable object to facilitate object handling
 */
public final class TransactionContext implements ITransactionContext {

    //private static final Deque<Set<IMultiVersionIndex>> INDEX_SET_BUFFER = new ConcurrentLinkedDeque<>();

    public final long tid;

    public final long lastTid;

    public final boolean readOnly;

    public final Set<IMultiVersionIndex> indexes;

    public TransactionContext(long tid, long lastTid, boolean readOnly) {
        this.tid = tid;
        this.lastTid = lastTid;
        this.readOnly = readOnly;

        Set<IMultiVersionIndex> chosenIndexSet;
        if(!readOnly) {
            // chosenIndexSet = INDEX_SET_BUFFER.poll();
            // 4 maximum indexes used per task
            // if(chosenIndexSet == null)
            chosenIndexSet = new HashSet<>(4);
        } else {
            chosenIndexSet = Collections.emptySet();
        }
        this.indexes = chosenIndexSet;
    }

    @Override
    public void close(){
        // not sure the reason for another thread acquiring and modifying the indexes set concurrently
        // with a running thread, leading to concurrent exception...
//        if(this.readOnly) return;
//        this.indexes.clear();
//        INDEX_SET_BUFFER.addLast(this.indexes);
    }

    public long getLastTid() {
        return lastTid;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public long getTid() {
        return tid;
    }
}
