package dk.ku.di.dms.vms.sdk.core.event.pubsub;

import dk.ku.di.dms.vms.modb.common.event.DataRequestEvent;
import dk.ku.di.dms.vms.modb.common.event.DataResponseEvent;
import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTaskResult;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionScheduler;

import java.util.Map;
import java.util.Queue;

/**
 * In a virtual microservice paradigm, internal components exchange a lot of internal events.
 * Some components require consuming from different streams (e.g. {@link VmsTransactionScheduler}.
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
 */
public interface IVmsInternalPubSub {

    /**
     * It represents events ready for scheduler consumption
     */
    Queue<TransactionalEvent> inputQueue();

    /**
     *  It represents events ready for delivery
     *  The payload handler thread consumes (and never inserts!) from this queue
     */
    Queue<TransactionalEvent> outputQueue();

    /**
     * It represents the queue holding the results of the submitted tasks
     * @return
     */
    Queue<VmsTransactionTaskResult> resultQueue();

    /**
     * A queue of requests for data
     * @return
     */
    Queue<DataRequestEvent> requestQueue();

    /**
     * A map of data request responses (keyed by thread identifier)
     * @return
     */
    Map<Long, DataResponseEvent> responseMap();

}
