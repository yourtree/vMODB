package dk.ku.di.dms.vms.modb.common.schema.network.meta;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Identification of a VMS that is ought to receive any sort of events
 * Contains only the necessary information for that
 * Attributes do not form an identification of the VMS, but rather
 * make it easier to manage metadata about each. I.e., metadata that
 * must be shared across threads (e.g., transactionEventsPerBatch),
 * managed by the event handler and coordinator (see comments on each attribute)
 */
public class ConsumerVms extends NetworkAddress {

    public transient final Map<Long, BlockingDeque<TransactionEvent.Payload>> transactionEventsPerBatch;

    /**
     * Timer for writing to each VMS connection
     * Read happens asynchronously anyway, so no need to set up timer for that
     * Only used by event handler {e.g., EmbedVmsEventHandler} to spawn periodical send of batch of events
      */
    public transient Timer timer;

    /**
     * Only used by Coordinator to send events to a VMS
     */
    public transient Runnable vmsWorker;

    public ConsumerVms(String host, int port) {
        super(host, port);
        this.transactionEventsPerBatch = new ConcurrentHashMap<>();
    }

    public ConsumerVms(SocketAddress address) {
        super(((InetSocketAddress)address).getHostName(), ((InetSocketAddress)address).getPort());
        this.transactionEventsPerBatch = new ConcurrentHashMap<>();
    }

}