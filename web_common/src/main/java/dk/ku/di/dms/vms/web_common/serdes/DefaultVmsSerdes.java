package dk.ku.di.dms.vms.web_common.serdes;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import dk.ku.di.dms.vms.modb.common.event.DataRequestEvent;
import dk.ku.di.dms.vms.modb.common.event.DataResponseEvent;
import dk.ku.di.dms.vms.modb.common.event.SystemEvent;
import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;
import dk.ku.di.dms.vms.web_common.meta.VmsDataSchema;
import dk.ku.di.dms.vms.web_common.meta.VmsEventSchema;

import java.nio.charset.StandardCharsets;
import java.util.Map;

class DefaultVmsSerdes implements IVmsSerdesProxy {

    private final Gson gson;

    public DefaultVmsSerdes(Gson gson) {
        this.gson = gson;
    }

    /**
     VMS METADATA EVENTS
     **/

    @Override
    public String serializeEventSchema(Map<String, VmsEventSchema> vmsEventSchema) {
        return gson.toJson( vmsEventSchema );
    }

    @Override
    public Map<String, VmsEventSchema> deserializeEventSchema(String vmsEventSchemaStr) {
        return gson.fromJson(vmsEventSchemaStr, new TypeToken<Map<String, VmsEventSchema>>(){}.getType());
    }

    @Override
    public String serializeDataSchema(VmsDataSchema vmsDataSchema) {
        return gson.toJson( vmsDataSchema );
    }

    @Override
    public VmsDataSchema deserializeDataSchema(String dataSchema) {
        return gson.fromJson(dataSchema, VmsDataSchema.class);
    }

    @Override
    public byte[] serializeSystemEvent(SystemEvent systemEvent) {
        return gson.toJson( systemEvent ).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public SystemEvent deserializeSystemEvent(byte[] bytes) {
        String json = new String( bytes );
        return gson.fromJson(json, SystemEvent.class);
    }

    /**
     TRANSACTIONAL EVENT
     **/

    @Override
    public byte[] serializeTransactionalEvent(TransactionalEvent event) {
        return gson.toJson(event).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public TransactionalEvent deserializeToTransactionalEvent(byte[] bytes) {
        String json = new String( bytes );
        return gson.fromJson(json, TransactionalEvent.class);
    }

    /**
        DATA
    **/

     @Override
    public byte[] serializeDataRequestEvent(DataRequestEvent event) {
        String json = gson.toJson(event);
        return json.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public DataRequestEvent deserializeDataRequestEvent(byte[] bytes) {
        String json = new String( bytes );
        return gson.fromJson(json, DataRequestEvent.class);
    }

    @Override
    public byte[] serializeDataResponseEvent(DataResponseEvent event) {
        String json = gson.toJson(event);
        return json.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public DataResponseEvent deserializeToDataResponseEvent(byte[] bytes) {
        String json = new String( bytes );
        return gson.fromJson(json, DataResponseEvent.class);
    }


    @Override
    public <K,V> String serializeMap( Map<K,V> map ){
        return gson.toJson( map );
    }

    @Override
    public <K,V> Map<K,V> deserializeMap(String mapStr){
         return gson.fromJson(mapStr, new TypeToken<Map<K, V>>(){}.getType());
    }

    @Override
    public <T> String serialize(T value, Class<T> clazz) {
        return gson.toJson( value, clazz );
    }

    @Override
    public <T> T deserialize(String valueStr, Class<T> clazz) {
        return gson.fromJson( valueStr, clazz );
    }


}
