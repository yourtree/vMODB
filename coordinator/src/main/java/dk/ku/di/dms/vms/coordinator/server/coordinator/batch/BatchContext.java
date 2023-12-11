package dk.ku.di.dms.vms.coordinator.server.coordinator.batch;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Data structure to keep data about a batch commit
 * The batch in progress (the first or the one waiting for being committed)
 * must be shared with vms workers
 */
public final class BatchContext {

    // no need non volatile. immutable
    public final long batchOffset;

    // set of terminal VMSs that has not voted yet
    public Set<String> missingVotes;

    public final Set<String> terminalVMSs;

    public Map<String, Long> lastTidOfBatchPerVms;

    public Map<String, Long> previousBatchPerVms;

    public long tidAborted;

    public long lastTid;

    public BatchContext(long batchOffset) {
        this.batchOffset = batchOffset;
        this.terminalVMSs = new HashSet<>();
    }

    // called when the batch is over
    public void seal(long lastTidOverall, Map<String, Long> lastTidOfBatchPerVms, Map<String, Long> previousBatchPerVms){
        this.lastTid = lastTidOverall;
        // immutable
        this.lastTidOfBatchPerVms = Map.copyOf(lastTidOfBatchPerVms);
        this.previousBatchPerVms = Map.copyOf(previousBatchPerVms);
        this.missingVotes = new HashSet<>(this.terminalVMSs);
    }

}