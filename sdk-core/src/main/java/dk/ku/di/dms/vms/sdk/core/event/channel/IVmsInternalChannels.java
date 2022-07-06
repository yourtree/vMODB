package dk.ku.di.dms.vms.sdk.core.event.channel;

import dk.ku.di.dms.vms.modb.common.event.DataRequestEvent;
import dk.ku.di.dms.vms.modb.common.event.DataResponseEvent;
import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTaskResult;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionScheduler;
import dk.ku.di.dms.vms.web_common.meta.schema.batch.BatchAbortRequest;
import dk.ku.di.dms.vms.web_common.meta.schema.batch.BatchCommitRequest;
import dk.ku.di.dms.vms.web_common.meta.schema.batch.BatchComplete;
import dk.ku.di.dms.vms.web_common.meta.schema.transaction.TransactionAbort;
import dk.ku.di.dms.vms.web_common.meta.schema.transaction.TransactionEvent;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

/**
 * In a virtual microservice paradigm, internal components exchange a lot of internal events.
 * Some components require consuming from different streams (e.g. {@link VmsTransactionScheduler}).
 * Other only publish (task) and others consume from one and publish to another.
 *
 * There are some limitations with Java interfaces. One of them is that a class cannot
 * implement the same interface twice, even though different types are used.
 *
 * Besides, the {@link java.util.concurrent.Flow} interface requires a concrete implementation
 * and tie together the consumer and producer with the "requires" method call to receive data.
 * I believe this request nature is not fruitful here.
 *
 * To decouple the components, this interface is used to hide the queue concrete implementations
 * and each component makes use of the queue of interest of its own work.
 *
 * In the end, these are just explicit channels for different threads to communicate with
 * each other. Basically a message-passing mechanism.
 *
 */
public interface IVmsInternalChannels {

    /*
     * TRANSACTIONAL EVENTS
     */

    /**
     * It represents events ready for scheduler consumption
     */
    BlockingQueue<TransactionEvent.Payload> transactionInputQueue();

    /**
     *  It represents events ready for delivery
     *  The payload handler thread consumes (and never inserts!) from this queue
     */
    BlockingQueue<OutboundEventResult> transactionOutputQueue();

    /**
     * BATCH COMMIT, ABORT EVENTS
     */

    // this should be sent by terminal vms
    BlockingQueue<BatchComplete.Payload> batchCompleteQueue();

    // abort a specific transaction from the batch and restart state from there
    // should maintain a MV scheme to avoid rolling back to the last committed state
    BlockingQueue<TransactionAbort.Payload> transactionAbortInputQueue();

    BlockingQueue<TransactionAbort.Payload> transactionAbortOutputQueue();

    /**
     *  This is sent by the leader by all non-terminal VMSs involved in the last batch commit
     */
    BlockingQueue<BatchCommitRequest.Payload> batchCommitQueue();

    // sent by the new leader
    BlockingQueue<BatchAbortRequest.Payload> batchAbortQueue();

    // no response, al vms will definitely commit. if there is crash, they rerun the events
    // methods must be deterministic if developers want to maintain the same state as run before the crash
//    BlockingQueue<BatchCommitResponse.Payload> batchCommitResponseQueue();

    /*
     * ACTION
     */

    /** Writer action **/
    BlockingQueue<byte> actionQueue();

    /**
     * It represents the queue holding the results of the submitted tasks
     *
     * What is the difference between the resultQueue and outputQueue?
     * The output queue represents the result of the function executed
     * whereas the result queue is the metadata regarding the output (TID and more)
     */
    Queue<VmsTransactionTaskResult> transactionResultQueue();

    /*
     * DATA --> only for dbms-service
     */

    /**
     * A queue of requests for data
     */
    Queue<DataRequestEvent> dataRequestQueue();

    /**
     * A map of data request responses (keyed by thread identifier)
     */
    Map<Long, DataResponseEvent> dataResponseMap();

}
