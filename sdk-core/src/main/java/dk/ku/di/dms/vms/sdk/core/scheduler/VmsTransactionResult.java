package dk.ku.di.dms.vms.sdk.core.scheduler;

import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;

import java.util.List;

public final class VmsTransactionResult implements IVmsTransactionResult {

    private final long tid;

    private final OutboundEventResult outboundEvent;

    public VmsTransactionResult(long tid, OutboundEventResult outboundEvent) {
        this.tid = tid;
        this.outboundEvent = outboundEvent;
    }

    @Override
    public long tid() {
        return this.tid;
    }

    @Override
    public OutboundEventResult getOutboundEventResult() {
        return this.outboundEvent;
    }

    @Override
    public List<OutboundEventResult> getOutboundEventResults() {
        return null;
    }
}