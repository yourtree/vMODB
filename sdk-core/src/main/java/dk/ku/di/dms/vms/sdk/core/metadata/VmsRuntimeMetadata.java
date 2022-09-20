package dk.ku.di.dms.vms.sdk.core.metadata;

import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataSchema;
import dk.ku.di.dms.vms.modb.common.schema.VmsEventSchema;
import dk.ku.di.dms.vms.modb.common.data_structure.IdentifiableNode;
import dk.ku.di.dms.vms.sdk.core.facade.IVmsRepositoryFacade;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionSignature;

import java.util.List;
import java.util.Map;

/**
 * A data class that stores the mappings between events, queues, and transactions
 *
 * Why data schema is a map?
 * Potentially a server could hold two (or more) VMSs. But for now only one.
 */
public record VmsRuntimeMetadata(

        Map<String, VmsDataSchema> dataSchema,
        Map<String, VmsEventSchema> inputEventSchema,
        Map<String, VmsEventSchema> outputEventSchema,

        Map<String, List<IdentifiableNode<VmsTransactionSignature>>> queueToVmsTransactionMap,
        Map<String, Class<?>> queueToEventMap,
        Map<Class<?>, String> eventToQueueMap,

        Map<String, Object> loadedVmsInstances,
        List<IVmsRepositoryFacade> repositoryFacades,
        Map<Class<?>,String> entityToTableNameMap

){}