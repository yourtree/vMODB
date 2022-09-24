package dk.ku.di.dms.vms.sdk.core.event.channel;

import dk.ku.di.dms.vms.modb.common.event.DataRequestEvent;
import dk.ku.di.dms.vms.modb.common.event.DataResponseEvent;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchAbortRequest;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitRequest;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *   This class has the objective to decouple completely the
 *   payload handler (responsible for receiving external events)
 *   and the vms executor (responsible for collecting these events
 *   and reasoning about their schedule).
 *   Particularly, this class contains a repository of events
 *   that are ready for processing, either in case of
 *   events triggering data operations and results of operations.
 *
 *   This class is inspired by the Proxy pattern:
 *   https://en.wikipedia.org/wiki/Proxy_pattern
 *
 *   TODO to transfer data from a h2 to another, we can modify the script command to
 *          generate the sql query we want and then we transfer the file created
 *
 *   FIXME the concept of a channel (in the context of coroutines) may help in the future
 *         https://kotlinlang.org/docs/channels.html
 *
 * Provide communication channel for threads.
 * Must replace the vms internal channels class
 * It is just the same instance along the program execution, can be static
 */
public final class VmsInternalChannels implements IVmsInternalChannels {

    public static VmsInternalChannels getInstance(){
        return INSTANCE;
    }

    private static final VmsInternalChannels INSTANCE;

    private static final BlockingQueue<TransactionEvent.Payload> transactionInputQueue;

    private static final BlockingQueue<OutboundEventResult> transactionOutputQueue;

    private static final BlockingQueue<TransactionAbort.Payload> transactionAbortInputQueue;

    private static final BlockingQueue<TransactionAbort.Payload> transactionAbortOutputQueue;

    private static final BlockingQueue<BatchCommitRequest.Payload> batchCommitQueue;

    private static final BlockingQueue<BatchAbortRequest.Payload> batchAbortQueue;

    private static final Queue<DataRequestEvent> requestQueue;

    private static final Map<Long, DataResponseEvent> responseMap;

    private static final AtomicBoolean batchCommitInCourse;

    private static final Lock lock;

    private static final Condition start;

    private static final Condition complete;

    static {
        INSTANCE = new VmsInternalChannels();

        /* transaction **/
        transactionInputQueue = new LinkedBlockingQueue<>();
        transactionOutputQueue = new LinkedBlockingQueue<>();

        /* abort **/
        transactionAbortInputQueue = new LinkedBlockingQueue<>();
        transactionAbortOutputQueue = new LinkedBlockingQueue<>();

        /* batch **/
        batchCommitQueue = new LinkedBlockingQueue<>();
        batchAbortQueue = new LinkedBlockingQueue<>();

        batchCommitInCourse = new AtomicBoolean(false);
        lock = new ReentrantLock();
        start = lock.newCondition();
        complete = lock.newCondition();

        requestQueue = new ConcurrentLinkedQueue<>();
        responseMap = new ConcurrentHashMap<>();
    }

    @Override
    public BlockingQueue<TransactionEvent.Payload> transactionInputQueue() {
        return transactionInputQueue;
    }

    @Override
    public BlockingQueue<OutboundEventResult> transactionOutputQueue() {
        return transactionOutputQueue;
    }

    @Override
    public BlockingQueue<TransactionAbort.Payload> transactionAbortInputQueue() {
        return transactionAbortInputQueue;
    }

    @Override
    public BlockingQueue<TransactionAbort.Payload> transactionAbortOutputQueue() {
        return transactionAbortOutputQueue;
    }

    @Override
    public BlockingQueue<BatchCommitRequest.Payload> batchCommitQueue() {
        return batchCommitQueue;
    }

    @Override
    public BlockingQueue<BatchAbortRequest.Payload> batchAbortQueue() {
        return batchAbortQueue;
    }

    @Override
    public Queue<DataRequestEvent> dataRequestQueue() {
        return requestQueue;
    }

    @Override
    public Map<Long, DataResponseEvent> dataResponseMap() {
        return responseMap;
    }


    @Override
    public AtomicBoolean batchCommitInCourse(){
        return batchCommitInCourse;
    }

    @Override
    public void signalCanStart() {
        lock.lock();
        start.signal();
        complete.awaitUninterruptibly();
        lock.unlock();
    }

    @Override
    public void waitForCanStartSignal() {
        lock.lock();
        start.awaitUninterruptibly();
    }

    @Override
    public void signalComplete() {
        complete.signal();
        lock.unlock();
    }
}
