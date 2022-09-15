package dk.ku.di.dms.vms.sdk.core.metadata;

import dk.ku.di.dms.vms.modb.common.schema.VmsDataSchema;
import dk.ku.di.dms.vms.modb.common.schema.VmsEventSchema;
import dk.ku.di.dms.vms.modb.common.data_structure.IdentifiableNode;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionSignature;

import java.util.List;
import java.util.Map;

/**
 * A data class that stores the mappings between events, queues, and transactions
 */
public record VmsRuntimeMetadata(
        Map<String, VmsDataSchema> vmsDataSchema, // solo schema
        Map<String, VmsEventSchema> vmsEventSchema,
        Map<String, List<IdentifiableNode<VmsTransactionSignature>>> queueToVmsTransactionMap,
        Map<String, Class<?>> queueToEventMap, // input
        Map<Class<?>,String> eventToQueueMap, // output
        Map<String, Object> loadedVmsInstances,
        Map<Class<?>,String> entityToTableNameMap
){}