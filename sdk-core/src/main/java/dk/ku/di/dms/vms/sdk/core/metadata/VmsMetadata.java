package dk.ku.di.dms.vms.sdk.core.metadata;

import dk.ku.di.dms.vms.web_common.modb.VmsDataSchema;
import dk.ku.di.dms.vms.web_common.modb.VmsEventSchema;
import dk.ku.di.dms.vms.modb.common.utils.IdentifiableNode;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionSignature;
import dk.ku.di.dms.vms.modb.common.event.IVmsApplicationEvent;

import java.util.List;
import java.util.Map;

/**
 * A data class that stores the mappings between events, queues, and transactions
 */
public record VmsMetadata (
        VmsDataSchema vmsDataSchema, // solo schema
        Map<String, VmsEventSchema> vmsEventSchema,
        Map<String, List<IdentifiableNode<VmsTransactionSignature>>> eventToVmsTransactionMap,
        Map<String, Class> queueToEventMap, // input
        Map<Class,String> eventToQueueMap, // output
        Map<String, Object> loadedVmsInstances
){}