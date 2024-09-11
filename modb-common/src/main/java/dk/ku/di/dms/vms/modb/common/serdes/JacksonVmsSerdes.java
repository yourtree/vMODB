package dk.ku.di.dms.vms.modb.common.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataModel;
import dk.ku.di.dms.vms.modb.common.schema.VmsEventSchema;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;

import java.util.List;
import java.util.Map;
import java.util.Set;

final class JacksonVmsSerdes implements IVmsSerdesProxy {

    private static final ObjectMapper mapper = new ObjectMapper().configure(MapperFeature.AUTO_DETECT_GETTERS, false);

    @Override
    public String serializeEventSchema(Map<String, VmsEventSchema> vmsEventSchema) {
        try {
            return mapper.writeValueAsString(vmsEventSchema);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static final TypeReference<Map<String, VmsEventSchema>> EVENT_SCHEMA_CLAZZ = new TypeReference<>() { };

    @Override
    public Map<String, VmsEventSchema> deserializeEventSchema(String json) {
        try {
            return mapper.readValue(json, EVENT_SCHEMA_CLAZZ);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static final TypeReference<Map<String, VmsDataModel>> DATA_MODEL_CLAZZ = new TypeReference<>() { };

    @Override
    public String serializeDataSchema(Map<String, VmsDataModel> vmsDataSchema) {
        try {
            return mapper.writeValueAsString(vmsDataSchema);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, VmsDataModel> deserializeDataSchema(String vmsDataSchema) {
        try {
            return mapper.readValue(vmsDataSchema, DATA_MODEL_CLAZZ);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <K, V> String serializeMap(Map<K, V> map) {
        try {
            return mapper.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <K, V> Map<K, V> deserializeMap(String mapStr) {
        try {
            TypeReference<Map<K, V>> clazz = new TypeReference<>() { };
            return mapper.readValue(mapStr, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <V> String serializeSet(Set<V> set) {
        try {
            return mapper.writeValueAsString(set);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <V> Set<V> deserializeSet(String setStr) {
        try {
            TypeReference<Set<V>> clazz = new TypeReference<>() { };
            return mapper.readValue(setStr, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static final TypeReference<Map<String, List<IdentifiableNode>>> CONSUMER_SET_CLAZZ = new TypeReference<>() { };

    @Override
    public String serializeConsumerSet(Map<String, List<IdentifiableNode>> map) {
        try {
            return mapper.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, List<IdentifiableNode>> deserializeConsumerSet(String mapStr) {
        try {
            return mapper.readValue(mapStr, CONSUMER_SET_CLAZZ);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static final TypeReference<Map<String, Long>> DEP_MAP_CLAZZ = new TypeReference<>() { };


    @Override
    public Map<String, Long> deserializeDependenceMap(String dependenceMapStr) {
        try {
            return mapper.readValue(dependenceMapStr, DEP_MAP_CLAZZ);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <V> String serializeList(List<V> list) {
        try {
            return mapper.writeValueAsString(list);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <V> List<V> deserializeList(String listStr) {
        try {
            TypeReference<List<V>> clazz = new TypeReference<>() { };
            return mapper.readValue(listStr, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String serialize(Object value, Class<?> clazz) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T deserialize(String valueStr, Class<T> clazz) {
        try {
            return mapper.readValue(valueStr, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
