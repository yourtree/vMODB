package dk.ku.di.dms.vms.sdk.embed;

import dk.ku.di.dms.vms.modb.catalog.Catalog;
import dk.ku.di.dms.vms.modb.common.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.common.event.IEvent;
import dk.ku.di.dms.vms.modb.store.table.Table;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionSignature;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A data class to store the mappings between events, queues, and operations
 */
public class VmsMetadataEmbed {

    private final Map<String, List<VmsTransactionSignature>> eventToOperationMap;
    private final Map<String,Class<IEvent>> queueToEventMap;
    private final Map<Class<IEvent>,String> eventToQueueMap;
    private final Map<String,Object> loadedMicroserviceClasses;
    private final Catalog catalog;

    // map entity clazz to table
    private Map<Class<? extends IEntity<?>>, Table> entityClazzToTableMap;

    public VmsMetadataEmbed() {
        this.eventToOperationMap = new HashMap<>();
        this.queueToEventMap = new HashMap<>();
        this.eventToQueueMap = new HashMap<>();
        this.loadedMicroserviceClasses = new HashMap<>();
        this.catalog = new Catalog();
        this.entityClazzToTableMap = new HashMap<>();
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

    public void registerEntityClazzMapToTable(final Class<? extends IEntity<?>> entityClazz, final Table table){
        this.entityClazzToTableMap.put( entityClazz, table );
    }

    public Table getTableByEntityClazz(Class<? extends IEntity<?>> entityClazz){
        return this.entityClazzToTableMap.getOrDefault( entityClazz, null );
    }

}
