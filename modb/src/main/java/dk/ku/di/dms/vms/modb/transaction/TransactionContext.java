package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.common.transaction.TransactionContextBase;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Closeable object to facilitate object handling
 */
public final class TransactionContext extends TransactionContextBase {

    private static final Deque<Set<IMultiVersionIndex>> INDEX_SET_BUFFER = new ConcurrentLinkedDeque<>();

    public final Set<IMultiVersionIndex> indexes;

    public TransactionContext(long tid, long lastTid, boolean readOnly) {
        super(tid, lastTid, readOnly);
        if(!readOnly) {
            // 4 maximum indexes used per task
            this.indexes = Objects.requireNonNullElseGet(INDEX_SET_BUFFER.poll(), HashSet::new);
        } else {
            this.indexes = Collections.emptySet();
        }
    }

    @Override
    public void close(){
        this.indexes.clear();
        INDEX_SET_BUFFER.addLast(this.indexes);
    }

}
