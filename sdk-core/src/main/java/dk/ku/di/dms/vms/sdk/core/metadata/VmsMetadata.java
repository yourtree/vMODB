package dk.ku.di.dms.vms.sdk.core.metadata;

import dk.ku.di.dms.vms.modb.common.utils.IdentifiableNode;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionSignature;
import dk.ku.di.dms.vms.modb.common.event.IEvent;

import java.util.List;
import java.util.Map;

/**
 * A data class that stores the mappings between events, queues, and transactions
 */
public record VmsMetadata (
        Map<String, VmsSchema> vmsSchema,
        Map<String, List<IdentifiableNode<VmsTransactionSignature>>> eventToVmsTransactionMap,
        Map<String, Class<? extends IEvent>> queueToEventMap,
        Map<Class<? extends IEvent>,String> eventToQueueMap,
        Map<String, Object> loadedVmsInstances)
{}