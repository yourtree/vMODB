package dk.ku.di.dms.vms.coordinator.server.infra;

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
    public Set<String> terminalVMSs;

    public final Map<String,Long> lastTidOfBatchPerVms;

    public BatchContext(long batchOffset, Map<String,Long> lastTidOfBatchPerVms) {
        this.batchOffset = batchOffset;
        this.lastTidOfBatchPerVms = Collections.unmodifiableMap(lastTidOfBatchPerVms);
        this.terminalVMSs = new HashSet<>();
    }

    // called when the batch is over
    public void seal(){
        this.missingVotes = Collections.synchronizedSet(terminalVMSs);
    }

}