package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

import java.util.Map;
import java.util.Timer;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Identification of a VMS that is ought to receive any sort of events
 * Contains only the necessary information for that
 * Attributes do not form an identification of the VMS, but rather
 * make it easier to manage metadata about each. I.e., metadata that
 * must be shared across threads (e.g., transactionEventsPerBatch)
 */
public final class ConsumerVms extends IdentifiableNode {

    transient final Map<Long, BlockingDeque<TransactionEvent.Payload>> transactionEventsPerBatch;

    /**
     * Timer for writing to each VMS connection
     * Read happens asynchronously anyway, so no need to set up timer for that
     * Only used by event handler {e.g., EmbedVmsEventHandler} to spawn periodical send of batch of events
      */
    public final transient Timer timer;

    public ConsumerVms(String identifier, String host, int port, Timer timer) {
        super(identifier, host, port);
        this.transactionEventsPerBatch = new ConcurrentHashMap<>();
        this.timer = timer;
    }

    public void addEventToBatch(long batchId, TransactionEvent.Payload eventPayload){
        this.transactionEventsPerBatch.computeIfAbsent(batchId, (x) -> new LinkedBlockingDeque<>()).add(eventPayload);
    }

}