package dk.ku.di.dms.vms.sdk.core.event.channel;

import dk.ku.di.dms.vms.modb.common.event.DataRequestEvent;
import dk.ku.di.dms.vms.modb.common.event.DataResponseEvent;
import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTaskResult;
import dk.ku.di.dms.vms.web_common.meta.schema.batch.BatchAbortRequest;
import dk.ku.di.dms.vms.web_common.meta.schema.batch.BatchCommitRequest;
import dk.ku.di.dms.vms.web_common.meta.schema.batch.BatchComplete;
import dk.ku.di.dms.vms.web_common.meta.schema.transaction.TransactionAbort;
import dk.ku.di.dms.vms.web_common.meta.schema.transaction.TransactionEvent;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

    private static final BlockingQueue<TransactionEvent.Payload> inputChannel;

    private static final BlockingQueue<OutboundEventResult> outputChannel;

    private static final Queue<VmsTransactionTaskResult> resultQueue;

    private static final Queue<DataRequestEvent> requestQueue;

    private static final Map<Long, DataResponseEvent> responseMap;

    private static final BlockingQueue<byte> actionQueue;

    static {
        INSTANCE = new VmsInternalChannels();
        inputChannel = new LinkedBlockingQueue<>();
        outputChannel = new LinkedBlockingQueue<>();
        resultQueue = new ConcurrentLinkedQueue<>();
        requestQueue = new ConcurrentLinkedQueue<>();
        responseMap = new ConcurrentHashMap<>();

        actionQueue = new LinkedBlockingQueue<byte>();
    }

    @Override
    public BlockingQueue<TransactionEvent.Payload> transactionInputQueue() {
        return inputChannel;
    }

    @Override
    public BlockingQueue<OutboundEventResult> transactionOutputQueue() {
        return outputChannel;
    }

    @Override
    public BlockingQueue<BatchComplete.Payload> batchCompleteQueue() {
        return null;
    }

    @Override
    public BlockingQueue<TransactionAbort.Payload> transactionAbortInputQueue() {
        return null;
    }

    @Override
    public BlockingQueue<TransactionAbort.Payload> transactionAbortOutputQueue() {
        return null;
    }


    @Override
    public BlockingQueue<BatchCommitRequest.Payload> batchCommitQueue() {
        return null;
    }

    @Override
    public BlockingQueue<BatchAbortRequest.Payload> batchAbortQueue() {
        return null;
    }

    @Override
    public BlockingQueue<byte> actionQueue() {
        return actionQueue;
    }


    @Override
    public Queue<VmsTransactionTaskResult> transactionResultQueue() {
        return resultQueue;
    }

    @Override
    public Queue<DataRequestEvent> dataRequestQueue() {
        return requestQueue;
    }

    @Override
    public Map<Long, DataResponseEvent> dataResponseMap() {
        return responseMap;
    }

}
