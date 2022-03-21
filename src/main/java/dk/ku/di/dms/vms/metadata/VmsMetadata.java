package dk.ku.di.dms.vms.metadata;

import dk.ku.di.dms.vms.database.catalog.Catalog;
import dk.ku.di.dms.vms.event.IEvent;
import dk.ku.di.dms.vms.operational.DataOperationSignature;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A data class to store the mappings between events, queues, and operations
 */
public class VmsMetadata {

    public final Map<String, List<DataOperationSignature>> eventToOperationMap;
    public final Map<String,Class<IEvent>> queueToEventMap;
    private final Map<String,Object> loadedMicroserviceClasses;
    private final Catalog catalog;

    public VmsMetadata() {
        this.eventToOperationMap = new HashMap<>();
        this.queueToEventMap = new HashMap<>();
        this.loadedMicroserviceClasses = new HashMap<>();
        this.catalog = new Catalog();
    }

    public <V> V getMicroservice(Class<V> clazz){
        String clazzName = clazz.getCanonicalName();
        return (V) this.loadedMicroserviceClasses.get(clazzName);
    }

    public <V> V getMicroservice(String name){
        return (V) this.loadedMicroserviceClasses.get(name);
    }

    public void registerMicroservice(String name, Object object){
        this.loadedMicroserviceClasses.put(name,object);
    }

    public Catalog getCatalog() {
        return catalog;
    }
}
