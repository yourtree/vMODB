package dk.ku.di.dms.vms.sdk.embed.channel;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.sdk.core.event.channel.IVmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionResult;
import dk.ku.di.dms.vms.sdk.embed.handler.BatchContext;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;

public class VmsEmbedInternalChannels implements IVmsInternalChannels {

    private final BlockingQueue<TransactionEvent.Payload> transactionInputQueue;

    private final BlockingQueue<VmsTransactionResult> transactionOutputQueue;

    private final BlockingQueue<TransactionAbort.Payload> transactionAbortInputQueue;

    private final BlockingQueue<TransactionAbort.Payload> transactionAbortOutputQueue;

    private final BlockingQueue<BatchContext> batchCommitRequestQueue;

    public VmsEmbedInternalChannels() {

        // linked blocking queue because method size is a constant time operation

        /* transaction **/
        this.transactionInputQueue = new LinkedBlockingQueue<>();
        this.transactionOutputQueue = new LinkedBlockingQueue<>();

        /* abort **/
        this.transactionAbortInputQueue = new LinkedBlockingQueue<>();
        this.transactionAbortOutputQueue = new LinkedBlockingQueue<>();

        /* batch */
        this.batchCommitRequestQueue = new LinkedBlockingQueue<>();

    }

    @Override
    public BlockingQueue<TransactionEvent.Payload> transactionInputQueue() {
        return this.transactionInputQueue;
    }

    @Override
    public BlockingQueue<VmsTransactionResult> transactionOutputQueue() {
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

    public BlockingQueue<BatchContext> batchCommitRequestQueue(){
        return this.batchCommitRequestQueue;
    }

}
