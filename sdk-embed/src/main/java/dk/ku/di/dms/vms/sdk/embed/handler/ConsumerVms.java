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

    transient final BlockingDeque<TransactionEvent.Payload> transactionEvents;

    public ConsumerVms(String identifier, String host, int port) {
        super(identifier, host, port);
        this.transactionEvents = new LinkedBlockingDeque<>();
    }

    public void addEventToBatch(TransactionEvent.Payload eventPayload){
        this.transactionEvents.add(eventPayload);
    }

}