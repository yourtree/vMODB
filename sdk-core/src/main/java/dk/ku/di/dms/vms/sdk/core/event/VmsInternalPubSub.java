package dk.ku.di.dms.vms.sdk.core.event;

import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTask;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *   This class has the objective to decouple completely the
 *   event handler (responsible for receiving external events)
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
    private final BlockingQueue<TransactionalEvent> inputQueue;

    /**
     *  It represents events ready for delivery
     *  The event handler threads consume from this queue
     */
    private final BlockingQueue<TransactionalEvent> outputQueue;

    /**
     * It represents events ready for execution
     */
    //final public BlockingQueue<VmsTransactionTask> readyQueue;

    public VmsInternalPubSub(){
        this.inputQueue = new LinkedBlockingQueue<>();
        this.outputQueue = new LinkedBlockingQueue<>();
        //this.readyQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public BlockingQueue<TransactionalEvent> inputQueue() {
        return inputQueue;
    }

    @Override
    public BlockingQueue<TransactionalEvent> outputQueue() {
        return outputQueue;
    }
}
