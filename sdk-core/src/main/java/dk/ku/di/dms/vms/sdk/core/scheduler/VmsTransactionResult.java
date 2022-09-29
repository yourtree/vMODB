package dk.ku.di.dms.vms.sdk.core.scheduler;

import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTaskResult;

import java.util.List;

/**
 * A collection of results of a given transaction (tid) in a specific VMS
 * It bundles everything necessary to commit this transaction as part of a batch
 */
public class VmsTransactionResult {

    public final long tid;

    public final List<OutboundEventResult> resultTasks;

    public VmsTransactionResult(long tid, List<OutboundEventResult> resultTasks) {
        this.tid = tid;
        this.resultTasks = resultTasks;
    }

}
