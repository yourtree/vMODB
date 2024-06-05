package dk.ku.di.dms.vms.sdk.embed.channel;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.sdk.core.channel.IVmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.core.scheduler.IVmsTransactionResult;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class VmsEmbeddedInternalChannels implements IVmsInternalChannels {

    private final Queue<InboundEvent> transactionInputQueue;

    private final Queue<IVmsTransactionResult> transactionOutputQueue;

    private final Queue<TransactionAbort.Payload> transactionAbortInputQueue;

    private final Queue<TransactionAbort.Payload> transactionAbortOutputQueue;

    private final Queue<Object> batchCommitCommandQueue;

    public VmsEmbeddedInternalChannels() {
        // linked blocking queue because method size is a constant time operation
        /* transaction **/
        this.transactionInputQueue = new ConcurrentLinkedQueue<>();
        this.transactionOutputQueue = new ConcurrentLinkedQueue<>();
        /* abort **/
        this.transactionAbortInputQueue = new ConcurrentLinkedQueue<>();
        this.transactionAbortOutputQueue = new ConcurrentLinkedQueue<>();
        /* batch */
        this.batchCommitCommandQueue = new ConcurrentLinkedQueue<>();
    }

    @Override
    public Queue<InboundEvent> transactionInputQueue() {
        return this.transactionInputQueue;
    }

    @Override
    public Queue<IVmsTransactionResult> transactionOutputQueue() {
        return this.transactionOutputQueue;
    }

    @Override
    public Queue<TransactionAbort.Payload> transactionAbortInputQueue() {
        return this.transactionAbortInputQueue;
    }

    @Override
    public Queue<TransactionAbort.Payload> transactionAbortOutputQueue() {
        return this.transactionAbortOutputQueue;
    }

    public Queue<Object> batchCommitCommandQueue(){
        return this.batchCommitCommandQueue;
    }

}
