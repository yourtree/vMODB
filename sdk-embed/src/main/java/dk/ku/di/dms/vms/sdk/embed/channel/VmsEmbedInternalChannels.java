package dk.ku.di.dms.vms.sdk.embed.channel;

import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitRequest;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.sdk.core.event.channel.IVmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;
import dk.ku.di.dms.vms.sdk.embed.scheduler.BatchContext;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class VmsEmbedInternalChannels implements IVmsInternalChannels {

    private final BlockingQueue<TransactionEvent.Payload> transactionInputQueue;

    private final BlockingQueue<OutboundEventResult> transactionOutputQueue;

    private final BlockingQueue<TransactionAbort.Payload> transactionAbortInputQueue;

    private final BlockingQueue<TransactionAbort.Payload> transactionAbortOutputQueue;

    private final BlockingQueue<BatchContext> batchContextQueue;

    private final BlockingQueue<BatchCommitRequest.Payload> newBatchCommitRequestQueue;

    public VmsEmbedInternalChannels() {

        /* transaction **/
        this.transactionInputQueue = new LinkedBlockingQueue<>();
        this.transactionOutputQueue = new LinkedBlockingQueue<>();

        /* abort **/
        this.transactionAbortInputQueue = new LinkedBlockingQueue<>();
        this.transactionAbortOutputQueue = new LinkedBlockingQueue<>();

        /* batch */
        this.batchContextQueue = new LinkedBlockingQueue<>();
        this.newBatchCommitRequestQueue = new LinkedBlockingQueue<>();

    }

    @Override
    public BlockingQueue<TransactionEvent.Payload> transactionInputQueue() {
        return this.transactionInputQueue;
    }

    @Override
    public BlockingQueue<OutboundEventResult> transactionOutputQueue() {
        return this.transactionOutputQueue;
    }

    @Override
    public BlockingQueue<TransactionAbort.Payload> transactionAbortInputQueue() {
        return this.transactionAbortInputQueue;
    }

    @Override
    public BlockingQueue<TransactionAbort.Payload> transactionAbortOutputQueue() {
        return this.transactionAbortOutputQueue;
    }

    public BlockingQueue<BatchContext> batchContextQueue(){
        return this.batchContextQueue;
    }

    public BlockingQueue<BatchCommitRequest.Payload> newBatchCommitRequestQueue(){
        return this.newBatchCommitRequestQueue;
    }

}
