package dk.ku.di.dms.vms.sdk.core.scheduler;

import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;

import java.util.List;

public interface IVmsTransactionResult {

    long tid();

    OutboundEventResult getOutboundEventResult();

    List<OutboundEventResult> getOutboundEventResults();

}
