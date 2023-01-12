package dk.ku.di.dms.vms.modb.common.schema.network.meta;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Identification of a VMS that is ought to receive any sort of events
 * Contains only the necessary information for that
 */
public class ConsumerVms extends NetworkNode {

    /** Attributes below do not form an identification of the VMS, but rather
     * make it easier to manage metadata about each
     */

    public transient final Map<Long, BlockingQueue<TransactionEvent.Payload>> transactionEventsPerBatch;

    public transient final List<TransactionEvent.Payload> pendingWrites;

    public ConsumerVms(String host, int port) {
        super(host, port);
        this.pendingWrites = new ArrayList<>(10);
        this.transactionEventsPerBatch = new ConcurrentHashMap<>();
    }

}
