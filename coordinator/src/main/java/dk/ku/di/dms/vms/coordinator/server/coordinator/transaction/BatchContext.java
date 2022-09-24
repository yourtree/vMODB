package dk.ku.di.dms.vms.coordinator.server.coordinator.transaction;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Data structure to keep data about a batch commit
 */
public final class BatchContext {

    // no need non volatile. immutable
    public final long batchOffset;

    // set of terminal VMSs that has not voted yet
    public Set<String> missingVotes;

    // may change across batches
    public final Set<String> terminalVMSs;

    public Map<String, Long> lastTidOfBatchPerVms;

    public BatchContext(long batchOffset) {
        this.batchOffset = batchOffset;
        this.terminalVMSs = new HashSet<>();
    }

    // called when the batch is over
    public void seal(Map<String, Long> lastTidOfBatchPerVms){
        this.lastTidOfBatchPerVms = Collections.unmodifiableMap(lastTidOfBatchPerVms);
        // synchronized because vote can be received by different threads
        this.missingVotes = Collections.synchronizedSet(terminalVMSs);
    }

}