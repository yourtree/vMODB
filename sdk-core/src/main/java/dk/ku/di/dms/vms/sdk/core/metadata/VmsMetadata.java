package dk.ku.di.dms.vms.sdk.core.metadata;

import dk.ku.di.dms.vms.web_common.meta.VmsDataSchema;
import dk.ku.di.dms.vms.web_common.meta.VmsEventSchema;
import dk.ku.di.dms.vms.modb.common.utils.IdentifiableNode;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionSignature;
import dk.ku.di.dms.vms.modb.common.event.IApplicationEvent;

import java.util.List;
import java.util.Map;

/**
 * A data class that stores the mappings between events, queues, and transactions
 */
public record VmsMetadata (
        Map<String, VmsDataSchema> vmsDataSchema,
        Map<String, VmsEventSchema> vmsEventSchema,
        Map<String, List<IdentifiableNode<VmsTransactionSignature>>> eventToVmsTransactionMap,
        Map<String, Class<? extends IApplicationEvent>> queueToEventMap,
        Map<Class<? extends IApplicationEvent>,String> eventToQueueMap,
        Map<String, Object> loadedVmsInstances
//        IVmsInternalPubSubService internalPubSubService // is this metadata?
){}