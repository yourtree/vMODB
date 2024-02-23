package dk.ku.di.dms.vms.sdk.core.scheduler;

import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;

import java.util.List;

/**
 * A collection of results of a given transaction (tid) in a specific VMS
 * It bundles everything necessary to commit this transaction as part of a batch
 */
public record VmsTransactionResult(long tid, List<OutboundEventResult> resultTasks) {

}
