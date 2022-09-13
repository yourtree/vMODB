package dk.ku.di.dms.vms.coordinator.server.coordinator.transaction;

import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;

public class TransactionManagerContext {

    // transaction manager actions
    // this provides a natural separation of tasks in the transaction manager thread.
    // commit handling, transaction parsing, leaving the main thread free
    // (only sending heartbeats)
    public BlockingQueue<Byte> actionQueue;

    // channels
    public Queue<BatchComplete.Payload> batchCompleteEvents;

    public Queue<TransactionAbort.Payload> transactionAbortEvents;

    public TransactionManagerContext(BlockingQueue<Byte> actionQueue, Queue<BatchComplete.Payload> batchCompleteEvents, Queue<TransactionAbort.Payload> transactionAbortEvents) {
        this.actionQueue = actionQueue;
        this.batchCompleteEvents = batchCompleteEvents;
        this.transactionAbortEvents = transactionAbortEvents;
    }

    public BlockingQueue<Byte> actionQueue() {
        return actionQueue;
    }

    public Queue<BatchComplete.Payload> batchCompleteEvents() {
        return batchCompleteEvents;
    }

    public Queue<TransactionAbort.Payload> transactionAbortEvents() {
        return transactionAbortEvents;
    }
}