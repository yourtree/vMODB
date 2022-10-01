package dk.ku.di.dms.vms.sdk.core.metadata;

import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataSchema;
import dk.ku.di.dms.vms.modb.common.schema.VmsEventSchema;
import dk.ku.di.dms.vms.sdk.core.facade.IVmsRepositoryFacade;

import javax.xml.catalog.Catalog;
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

        Map<String, VmsTransactionMetadata> queueToVmsTransactionMap,
        Map<String, Class<?>> queueToEventMap,
        Map<Class<?>, String> eventToQueueMap,

        Map<String, Object> loadedVmsInstances,
        List<IVmsRepositoryFacade> repositoryFacades,
        Map<Class<?>, String> entityToTableNameMap,

        Map<String, SelectStatement> staticQueries

){}