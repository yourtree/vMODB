package dk.ku.di.dms.vms.sdk.core.event.pubsub;

import dk.ku.di.dms.vms.modb.common.event.DataRequestEvent;
import dk.ku.di.dms.vms.modb.common.event.DataResponseEvent;
import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTaskResult;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

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
 */
public final class VmsInternalPubSub implements IVmsInternalPubSubService {

    /**
     * It represents events ready for scheduler consumption
     */
    private final Queue<TransactionalEvent> inputQueue;

    /**
     *  It represents events ready for delivery
     *  The payload handler threads consume from this queue
     */
    private final Queue<TransactionalEvent> outputQueue;

    /**
     * It represents the result of tasks
     */
    private final Queue<VmsTransactionTaskResult> resultQueue;

    /**
     * The requests for data or DBMS statements that should be delivered
     * @return
     */
    private final Queue<DataRequestEvent> requestQueue;

    private final Map<Long,DataResponseEvent> responseMap;

    public static VmsInternalPubSub newInstance(){
        return new VmsInternalPubSub();
    }

    private VmsInternalPubSub(){
        this.inputQueue = new LinkedList<>();
        this.outputQueue = new LinkedList<>();
        this.resultQueue = new LinkedList<>();
        this.requestQueue = new LinkedList<>();
        this.responseMap = new HashMap<>();
    }

    @Override
    public Queue<TransactionalEvent> inputQueue() {
        return inputQueue;
    }

    @Override
    public Queue<TransactionalEvent> outputQueue() {
        return outputQueue;
    }

    @Override
    public Queue<VmsTransactionTaskResult> resultQueue() {
        return resultQueue;
    }

    @Override
    public Queue<DataRequestEvent> requestQueue() {
        return requestQueue;
    }

    @Override
    public Map<Long, DataResponseEvent> responseMap() {
        return responseMap;
    }


}
