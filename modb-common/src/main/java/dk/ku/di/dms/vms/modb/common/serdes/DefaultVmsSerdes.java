package dk.ku.di.dms.vms.modb.common.serdes;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import dk.ku.di.dms.vms.modb.common.event.DataRequestEvent;
import dk.ku.di.dms.vms.modb.common.event.DataResponseEvent;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataSchema;
import dk.ku.di.dms.vms.modb.common.schema.VmsEventSchema;

import java.nio.charset.StandardCharsets;
import java.util.List;
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
    public String serializeDataSchema(Map<String, VmsDataSchema> vmsDataSchema) {
        return gson.toJson( vmsDataSchema );
    }

    @Override
    public Map<String, VmsDataSchema> deserializeDataSchema(String dataSchemaStr) {
        return gson.fromJson(dataSchemaStr, new TypeToken<Map<String, VmsDataSchema>>(){}.getType());
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
        return gson.toJson( map, new TypeToken<Map<K, V>>(){}.getType() );
    }

    @Override
    public <K,V> Map<K,V> deserializeMap(String mapStr){
         return gson.fromJson(mapStr, new TypeToken<Map<K, V>>(){}.getType());
    }

    @Override
    public <V> String serializeList(List<V> list) {
        return gson.toJson( list, new TypeToken<List<V>>(){}.getType() );
    }

    @Override
    public <V> List<V> deserializeList(String listStr) {
        return gson.fromJson(listStr, new TypeToken<List<V>>(){}.getType());
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
