package dk.ku.di.dms.vms.web_common.serdes;

import dk.ku.di.dms.vms.modb.common.event.DataRequestEvent;
import dk.ku.di.dms.vms.modb.common.event.DataResponseEvent;
import dk.ku.di.dms.vms.modb.common.event.SystemEvent;
import dk.ku.di.dms.vms.web_common.meta.VmsDataSchema;
import dk.ku.di.dms.vms.web_common.meta.VmsEventSchema;

import java.util.Map;

/**
 * A proxy for all types of events exchanged between the sdk and the sidecar
 * Used for complex objects like schema definitions
 */
public interface IVmsSerdesProxy {

    String serializeEventSchema(Map<String, VmsEventSchema> vmsEventSchema);
    Map<String, VmsEventSchema> deserializeEventSchema(String json);

    String serializeDataSchema(VmsDataSchema vmsDataSchema);
    VmsDataSchema deserializeDataSchema(String vmsDataSchema);

    /** System event... do we need that? **/
    byte[] serializeSystemEvent(SystemEvent systemEvent);
    SystemEvent deserializeSystemEvent(byte[] bytes);

    /**
     * A transactional event serves for both input and output
     */
//    byte[] serializeTransactionalEvent(TransactionalEvent event);
//    TransactionalEvent deserializeToTransactionalEvent(byte[] bytes);

    byte[] serializeDataRequestEvent(DataRequestEvent event);
    DataRequestEvent deserializeDataRequestEvent(byte[] bytes);

    byte[] serializeDataResponseEvent(DataResponseEvent event);
    DataResponseEvent deserializeToDataResponseEvent(byte[] bytes);

    <K,V> String serializeMap( Map<K,V> map );
    <K,V> Map<K,V> deserializeMap(String mapStr);

    <T> String serialize( T value, Class<T> clazz );
    <T> T deserialize( String valueStr, Class<T> clazz );
}
