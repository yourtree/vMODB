package dk.ku.di.dms.vms.tpcc.proxy.storage;

import dk.ku.di.dms.vms.modb.common.schema.VmsDataModel;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadataLoader;
import dk.ku.di.dms.vms.sdk.embed.entity.EntityHandler;
import dk.ku.di.dms.vms.sdk.embed.metadata.EmbedMetadataLoader;
import dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenerator;
import dk.ku.di.dms.vms.tpcc.proxy.entities.*;
import dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants;
import org.reflections.Reflections;

import java.io.File;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import static java.lang.System.Logger.Level.INFO;

public final class StorageUtils {

    private static final System.Logger LOGGER = System.getLogger(StorageUtils.class.getName());

    private static final Map<Class<?>, String> ENTITY_TO_VMS_MAP;
    static {
        ENTITY_TO_VMS_MAP = new HashMap<>();
        ENTITY_TO_VMS_MAP.put(Warehouse.class, "proxy");
        ENTITY_TO_VMS_MAP.put(District.class, "proxy");
        ENTITY_TO_VMS_MAP.put(Customer.class, "proxy");
        ENTITY_TO_VMS_MAP.put(Item.class, "proxy");
        ENTITY_TO_VMS_MAP.put(Stock.class, "proxy");
    }

    public static int getNumRecordsFromInDiskTable(Schema schema, String fileName){
        File file = EmbedMetadataLoader.buildFile(fileName);
        return (int) file.length() / schema.getRecordSize();
    }

    /**
     * Map already created tables in disk
     */
    public static Map<String, UniqueHashBufferIndex> mapTablesInDisk(EntityMetadata metadata, int numWare) {
        Map<String, UniqueHashBufferIndex> tableToIndexMap = new HashMap<>();
        for (var entry : metadata.entityToTableNameMap.entrySet()) {
            final Schema schema = metadata.entityToSchemaMap.get(entry.getValue());
            switch (entry.getValue()){
                case "warehouse" -> {
                    LOGGER.log(INFO, "Loading "+numWare+" warehouses...");
                    UniqueHashBufferIndex idx = buildHashIndex(entry.getValue(), schema, numWare, false);
                    tableToIndexMap.put(entry.getValue(), idx);
                }
                case "district" -> {
                    int maxRecords = numWare * TPCcConstants.NUM_DIST_PER_WARE;
                    LOGGER.log(INFO, "Loading "+maxRecords+" districts...");
                    UniqueHashBufferIndex idx = buildHashIndex(entry.getValue(), schema, maxRecords, false);
                    tableToIndexMap.put(entry.getValue(), idx);
                }
                case "customer" -> {
                    int maxRecords = numWare * TPCcConstants.NUM_DIST_PER_WARE * TPCcConstants.NUM_CUST_PER_DIST;
                    LOGGER.log(INFO, "Loading "+maxRecords+" customers...");
                    int overflowDisk = maxRecords * 3;
                    UniqueHashBufferIndex idx = buildHashIndex(entry.getValue(), schema, overflowDisk, false);
                    tableToIndexMap.put(entry.getValue(), idx);
                }
                case "item" -> {
                    LOGGER.log(INFO, "Loading "+TPCcConstants.NUM_ITEMS+" items...");
                    UniqueHashBufferIndex idx = buildHashIndex(entry.getValue(), schema, TPCcConstants.NUM_ITEMS, false);
                    tableToIndexMap.put(entry.getValue(), idx);
                }
                case "stock" -> {
                    int maxRecords = numWare * TPCcConstants.NUM_ITEMS;
                    int overflowDisk = maxRecords * 2;
                    LOGGER.log(INFO, "Loading "+maxRecords+" stock items...");
                    UniqueHashBufferIndex idx = buildHashIndex(entry.getValue(), schema, overflowDisk, false);
                    tableToIndexMap.put(entry.getValue(), idx);
                }
            }
        }
        return tableToIndexMap;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static EntityMetadata loadEntityMetadata() throws NoSuchFieldException, IllegalAccessException {
        Reflections reflections = VmsMetadataLoader.configureReflections(new String[]{
                "dk.ku.di.dms.vms.tpcc.proxy",
        });
        Map<Class<?>, String> entityToTableNameMap = VmsMetadataLoader.loadVmsTableNames(reflections);
        Map<String, VmsDataModel> vmsDataModelMap = VmsMetadataLoader.buildVmsDataModel(ENTITY_TO_VMS_MAP, entityToTableNameMap );
        Map<String, EntityHandler> entityHandlerMap = new HashMap<>();
        Map<String, Schema> entityToSchemaMap = new HashMap<>();
        for (var entry : entityToTableNameMap.entrySet()) {
            VmsDataModel vmsDataModel = vmsDataModelMap.get(entry.getValue());
            Class<?> entityClazz = entry.getKey();
            Type[] types = ((ParameterizedType) entityClazz.getGenericInterfaces()[0]).getActualTypeArguments();
            Class<?> pkClazz = (Class<?>) types[0];
            final Schema schema = EmbedMetadataLoader.buildEntitySchema(vmsDataModel, entityClazz);
            entityToSchemaMap.put(entry.getValue(), schema);
            EntityHandler entityHandler = new EntityHandler(pkClazz, entityClazz, schema);
            entityHandlerMap.put(entry.getValue(), entityHandler);
        }
        return new EntityMetadata(entityToTableNameMap, vmsDataModelMap, entityHandlerMap, entityToSchemaMap);
    }

    @SuppressWarnings({"rawtypes"})
    public record EntityMetadata(Map<Class<?>, String> entityToTableNameMap, Map<String, VmsDataModel> vmsDataModelMap, Map<String, EntityHandler> entityHandlerMap, Map<String,Schema> entityToSchemaMap){}

    /**
     * Generate data and create tables in disk
     */

    public static Map<String, UniqueHashBufferIndex> createTables(EntityMetadata metadata, int numWare) {
        Map<String, UniqueHashBufferIndex> tableToIndexMap = new HashMap<>();
        for (String tableName : metadata.entityToTableNameMap.values()) {
            UniqueHashBufferIndex idx = createTable(metadata, numWare, tableName);
            tableToIndexMap.put(tableName, idx);
        }
        return tableToIndexMap;
    }

    @SuppressWarnings("unchecked")
    public static UniqueHashBufferIndex createTable(EntityMetadata metadata, int numWare, String tableName) {
        final Schema schema = metadata.entityToSchemaMap.get(tableName);
        var entityHandler = metadata.entityHandlerMap.get(tableName);
        // generate data and store in respective indexes
        switch (tableName){
            case "warehouse" -> {
                LOGGER.log(INFO, "Creating "+ numWare +" warehouse records...");
                long initTs = System.currentTimeMillis();
                UniqueHashBufferIndex idx = buildHashIndex(tableName, schema, numWare, true);
                for(int w_id = 1; w_id <= numWare; w_id++){
                    Warehouse warehouse = DataGenerator.generateWarehouse(w_id);
                    Object[] record = entityHandler.extractFieldValuesFromEntityObject(warehouse);
                    IKey key = KeyUtils.buildRecordKey(schema.getPrimaryKeyColumns(), record);
                    idx.insert(key, record);
                }
                idx.flush();
                long endTs = System.currentTimeMillis();
                LOGGER.log(INFO, "Finished creating "+ numWare +" warehouse records in "+(endTs-initTs)+" ms");
                return idx;
            }
            case "district" -> {
                int maxRecords = numWare * TPCcConstants.NUM_DIST_PER_WARE;
                LOGGER.log(INFO, "Creating "+maxRecords+" district records...");
                long initTs = System.currentTimeMillis();
                UniqueHashBufferIndex idx = buildHashIndex(tableName, schema, maxRecords, true);
                for(int w_id = 1; w_id <= numWare; w_id++){
                    for(int d_id = 1; d_id <= TPCcConstants.NUM_DIST_PER_WARE; d_id++) {
                        District district = DataGenerator.generateDistrict(d_id, w_id);
                        Object[] record = entityHandler.extractFieldValuesFromEntityObject(district);
                        IKey key = KeyUtils.buildRecordKey(schema.getPrimaryKeyColumns(), record);
                        idx.insert(key, record);
                    }
                }
                idx.flush();
                long endTs = System.currentTimeMillis();
                LOGGER.log(INFO, "Finished creating "+maxRecords+" district records in "+(endTs-initTs)+" ms");
                return idx;
            }
            case "customer" -> {
                int maxRecords = numWare * TPCcConstants.NUM_DIST_PER_WARE * TPCcConstants.NUM_CUST_PER_DIST;
                LOGGER.log(INFO, "Creating "+maxRecords+" customer records...");
                int overflowDisk = maxRecords * 3;
                long initTs = System.currentTimeMillis();
                UniqueHashBufferIndex idx = buildHashIndex(tableName, schema, overflowDisk, true);
                for(int w_id = 1; w_id <= numWare; w_id++){
                    for(int d_id = 1; d_id <= TPCcConstants.NUM_DIST_PER_WARE; d_id++) {
                        for(int c_id = 1; c_id <= TPCcConstants.NUM_CUST_PER_DIST; c_id++) {
                            Customer customer = DataGenerator.generateCustomer(c_id, d_id, w_id);
                            Object[] record = entityHandler.extractFieldValuesFromEntityObject(customer);
                            IKey key = KeyUtils.buildRecordKey(schema.getPrimaryKeyColumns(), record);
                            idx.insert(key, record);
                        }
                    }
                }
                idx.flush();
                long endTs = System.currentTimeMillis();
                LOGGER.log(INFO, "Finished creating "+maxRecords+" customer records in "+(endTs-initTs)+" ms");
                return idx;
            }
            case "item" -> {
                LOGGER.log(INFO, "Creating "+TPCcConstants.NUM_ITEMS+" item records...");
                long initTs = System.currentTimeMillis();
                UniqueHashBufferIndex idx = buildHashIndex(tableName, schema, TPCcConstants.NUM_ITEMS, true);
                for(int i_id = 1; i_id <= TPCcConstants.NUM_ITEMS; i_id++){
                    Item item = DataGenerator.generateItem(i_id);
                    Object[] record = entityHandler.extractFieldValuesFromEntityObject(item);
                    IKey key = KeyUtils.buildRecordKey(schema.getPrimaryKeyColumns(), record);
                    idx.insert(key, record);
                }
                idx.flush();
                long endTs = System.currentTimeMillis();
                LOGGER.log(INFO, "Finished creating "+TPCcConstants.NUM_ITEMS+" item records in "+(endTs-initTs)+" ms");
                return idx;
            }
            case "stock" -> {
                int maxRecords = numWare * TPCcConstants.NUM_ITEMS;
                LOGGER.log(INFO, "Creating "+maxRecords+" stock records...");
                int overflowDisk = maxRecords * 2;
                long initTs = System.currentTimeMillis();
                UniqueHashBufferIndex idx = buildHashIndex(tableName, schema, overflowDisk, true);
                for(int w_id = 1; w_id <= numWare; w_id++) {
                    for (int i_id = 1; i_id <= TPCcConstants.NUM_ITEMS; i_id++) {
                        Stock stock = DataGenerator.generateStockItem(w_id, i_id);
                        Object[] record = entityHandler.extractFieldValuesFromEntityObject(stock);
                        IKey key = KeyUtils.buildRecordKey(schema.getPrimaryKeyColumns(), record);
                        idx.insert(key, record);
                    }
                }
                idx.flush();
                long endTs = System.currentTimeMillis();
                LOGGER.log(INFO, "Finished creating "+maxRecords+" stock records in "+(endTs-initTs)+" ms");
                return idx;
            }
            case null, default -> throw new RuntimeException("Table name "+tableName+" not identified!");
        }
    }

    public static UniqueHashBufferIndex buildHashIndex(String tableName, Schema schema, int maxRecords, boolean truncate) {
        RecordBufferContext recordBufferContext = EmbedMetadataLoader.loadRecordBuffer(maxRecords, schema.getRecordSizeWithHeader(), tableName, truncate);
        return new UniqueHashBufferIndex(recordBufferContext, schema, schema.getPrimaryKeyColumns(), maxRecords);
    }

}
