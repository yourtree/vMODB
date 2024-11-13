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
import dk.ku.di.dms.vms.tpcc.inventory.entities.Item;
import dk.ku.di.dms.vms.tpcc.inventory.entities.Stock;
import dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenerator;
import dk.ku.di.dms.vms.tpcc.proxy.datagen.TPCcConstants;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.Customer;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.District;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.Warehouse;
import org.reflections.Reflections;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import static java.lang.System.Logger.Level.INFO;

public final class StorageUtils {

    private static final System.Logger LOGGER = System.getLogger(StorageUtils.class.getName());

    /**
     * Map already created tables in disk
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Map<String, UniqueHashBufferIndex> mapTables() throws NoSuchFieldException, IllegalAccessException {

        return null;
    }

    /**
     * Generate data and create tables in disk
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void createTables() throws NoSuchFieldException, IllegalAccessException {

        Reflections reflections = VmsMetadataLoader.configureReflections(new String[]{
                "dk.ku.di.dms.vms.tpcc",
        });

        Map<Class<?>, String> entityToTableNameMap = VmsMetadataLoader.loadVmsTableNames(reflections);

        Map<Class<?>, String> entityToVms = new HashMap<>();
        entityToVms.put( Warehouse.class, "proxy" );
        entityToVms.put( District.class, "proxy" );
        entityToVms.put( Customer.class, "proxy" );
        entityToVms.put( Item.class, "proxy" );
        entityToVms.put( Stock.class, "proxy" );

        Map<String, VmsDataModel> vmsDataModelMap = VmsMetadataLoader.buildVmsDataModel( entityToVms, entityToTableNameMap );

        Map<String, EntityHandler> entityHandlerMap = new HashMap<>();

        for (var entry : entityToTableNameMap.entrySet()) {
            VmsDataModel vmsDataModel = vmsDataModelMap.get(entry.getValue());
            Class<?> entityClazz = entry.getKey();
            Type[] types = ((ParameterizedType) entityClazz.getGenericInterfaces()[0]).getActualTypeArguments();
            Class<?> pkClazz = (Class<?>) types[0];

            final Schema schema = EmbedMetadataLoader.buildEntitySchema(vmsDataModel, entityClazz);

            var entityHandler = new EntityHandler(pkClazz, entityClazz, schema);
            entityHandlerMap.put(entry.getValue(), entityHandler);

            // generate data and store in respective indexes
            switch (entry.getValue()){
                case "warehouse" -> {
                    UniqueHashBufferIndex idx = buildHashIndex(entry, schema, TPCcConstants.NUM_WARE);
                    for(int w_id = 1; w_id <= TPCcConstants.NUM_WARE; w_id++){
                        Warehouse warehouse = DataGenerator.generateWarehouse(w_id);
                        var record = entityHandler.extractFieldValuesFromEntityObject(warehouse);
                        IKey key = KeyUtils.buildRecordKey(schema.getPrimaryKeyColumns(), record);
                        idx.insert(key, record);
                    }
                    idx.flush();
                }
                case "district" -> {
                    int maxRecords = TPCcConstants.NUM_WARE * TPCcConstants.NUM_DIST_PER_WARE;
                    UniqueHashBufferIndex idx = buildHashIndex(entry, schema, maxRecords);
                    for(int w_id = 1; w_id <= TPCcConstants.NUM_WARE; w_id++){
                        for(int d_id = 1; d_id <= TPCcConstants.NUM_DIST_PER_WARE; d_id++) {
                            District district = DataGenerator.generateDistrict(d_id, w_id);
                            var record = entityHandler.extractFieldValuesFromEntityObject(district);
                            IKey key = KeyUtils.buildRecordKey(schema.getPrimaryKeyColumns(), record);
                            idx.insert(key, record);
                        }
                    }
                    idx.flush();
                }
                case "customer" -> {
                    int maxRecords = TPCcConstants.NUM_WARE * TPCcConstants.NUM_DIST_PER_WARE * TPCcConstants.NUM_CUST_PER_DIST * 2;
                    LOGGER.log(INFO, "Creating customer table with max records = "+maxRecords);

                    UniqueHashBufferIndex idx = buildHashIndex(entry, schema, maxRecords);
                    for(int w_id = 1; w_id <= TPCcConstants.NUM_WARE; w_id++){
                        for(int d_id = 1; d_id <= TPCcConstants.NUM_DIST_PER_WARE; d_id++) {
                            for(int c_id = 1; c_id <= TPCcConstants.NUM_CUST_PER_DIST; c_id++) {
                                Customer customer = DataGenerator.generateCustomer(d_id, w_id, c_id);
                                var record = entityHandler.extractFieldValuesFromEntityObject(customer);
                                IKey key = KeyUtils.buildRecordKey(schema.getPrimaryKeyColumns(), record);
                                idx.insert(key, record);
                            }
                        }
                    }
                    idx.flush();
                }
                case "item" -> {
                    UniqueHashBufferIndex idx = buildHashIndex(entry, schema, TPCcConstants.NUM_ITEMS);
                    for(int i_id = 1; i_id <= TPCcConstants.NUM_ITEMS; i_id++){
                        Item item = DataGenerator.generateItem(i_id);
                        var record = entityHandler.extractFieldValuesFromEntityObject(item);
                        IKey key = KeyUtils.buildRecordKey(schema.getPrimaryKeyColumns(), record);
                        idx.insert(key, record);
                    }
                    idx.flush();
                }
                case "stock" -> {
                    int maxRecords = TPCcConstants.NUM_WARE * TPCcConstants.NUM_ITEMS;
                    UniqueHashBufferIndex idx = buildHashIndex(entry, schema, maxRecords);
                    for(int w_id = 1; w_id <= TPCcConstants.NUM_WARE; w_id++) {
                        for (int i_id = 1; i_id <= TPCcConstants.NUM_ITEMS; i_id++) {
                            Stock stock = DataGenerator.generateStockItem(w_id, i_id);
                            var record = entityHandler.extractFieldValuesFromEntityObject(stock);
                            IKey key = KeyUtils.buildRecordKey(schema.getPrimaryKeyColumns(), record);
                            idx.insert(key, record);
                        }
                    }
                    idx.flush();
                }
            }
        }
    }

    private static UniqueHashBufferIndex buildHashIndex(Map.Entry<Class<?>, String> entry, Schema schema, int maxRecords) {
        RecordBufferContext recordBufferContext = EmbedMetadataLoader.loadRecordBuffer(maxRecords, schema.getRecordSizeWithHeader(), entry.getValue());
        return new UniqueHashBufferIndex(recordBufferContext, schema, schema.getPrimaryKeyColumns(), maxRecords);
    }

}
