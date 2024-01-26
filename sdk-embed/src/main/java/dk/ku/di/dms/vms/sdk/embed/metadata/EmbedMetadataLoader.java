package dk.ku.di.dms.vms.sdk.embed.metadata;

import dk.ku.di.dms.vms.modb.common.constraint.ForeignKeyReference;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataModel;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashIndex;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashMapIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashMapIndex;
import dk.ku.di.dms.vms.modb.storage.record.AppendOnlyBuffer;
import dk.ku.di.dms.vms.modb.storage.record.OrderedRecordBuffer;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;
import dk.ku.di.dms.vms.modb.transaction.OperationalAPI;
import dk.ku.di.dms.vms.modb.transaction.multiversion.IntegerPrimaryKeyGenerator;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.NonUniqueSecondaryIndex;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.PrimaryIndex;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy;

import javax.persistence.GeneratedValue;
import java.io.File;
import java.io.IOException;
import java.lang.ref.Cleaner;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.logging.Logger.GLOBAL_LOGGER_NAME;
import static java.util.logging.Logger.getLogger;

public class EmbedMetadataLoader {

    private static final Logger logger = getLogger(GLOBAL_LOGGER_NAME);

    private static final boolean IN_MEMORY_STORAGE = true;

    public static Map<String, Object> loadRepositoryClasses(Set<Class<?>> vmsClasses,
                                                                      Map<Class<?>, String> entityToTableNameMap,
                                                                      Map<String, Table> catalog,
                                                                      OperationalAPI operationalAPI) throws InvocationTargetException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        Map<String, Object> tableToRepositoryMap = new HashMap<>();
        for(Class<?> clazz : vmsClasses) {

            String clazzName = clazz.getCanonicalName();

            Class<?> cls = Class.forName(clazzName);
            Constructor<?>[] constructors = cls.getDeclaredConstructors();
            Constructor<?> constructor = constructors[0];

            // the IRepository required for this vms class
            Class<?>[] repositoryTypes = constructor.getParameterTypes();

            for (Class<?> repositoryType : repositoryTypes) {

                Type[] types = getPkAndEntityTypesFromRepositoryClazz(repositoryType);

                Class<?> pkClazz = (Class<?>) types[0];
                Class<?> entityClazz = (Class<?>) types[1];

                String tableName = entityToTableNameMap.get(entityClazz);

                // generate type
                TypeDescription.Generic generic = TypeDescription.Generic.Builder
                        .parameterizedType(AbstractProxyRepository.class, pkClazz, entityClazz).build();

                Class<?> type;
                try (DynamicType.Unloaded<?> dynamicType = new ByteBuddy()
                        .subclass(generic, ConstructorStrategy.Default.IMITATE_SUPER_CLASS)
                        .implement(repositoryType)
                        .name(repositoryType.getSimpleName().replaceFirst("I","") + "Impl")
                        .make()) {
                    type = dynamicType
                            .load(ClassLoader.getSystemClassLoader(), ClassLoadingStrategy.Default.INJECTION)
                            .getLoaded();
                }

                Object instance = type.getConstructors()[0].newInstance(
                        pkClazz,
                        entityClazz,
                        catalog.get( tableName ),
                        operationalAPI
                );

                tableToRepositoryMap.put( tableName, instance);

            }
        }
        return tableToRepositoryMap;
    }


    /**
     * Key: clazzName (annotated with @Microservice)
     */
    public static Map<String, List<Object>> mapRepositoriesToVms(
                Set<Class<?>> vmsClasses,
                Map<Class<?>, String> entityToTableNameMap,
                Map<String, Object> tableToRepositoryMap)
            throws ClassNotFoundException {
        Map<String, List<Object>> repositoryClassMap = new HashMap<>();
        for(Class<?> clazz : vmsClasses) {

            String clazzName = clazz.getCanonicalName();

            Class<?> cls = Class.forName(clazzName);
            Constructor<?>[] constructors = cls.getDeclaredConstructors();
            Constructor<?> constructor = constructors[0];

            // the IRepository required for this vms class
            Class<?>[] repositoryTypes = constructor.getParameterTypes();
            List<Object> proxies = new ArrayList<>(repositoryTypes.length);

            for (Class<?> repositoryType : repositoryTypes) {

                Type[] types = getPkAndEntityTypesFromRepositoryClazz(repositoryType);

                Class<?> pkClazz = (Class<?>) types[0];
                Class<?> entityClazz = (Class<?>) types[1];

                String tableName = entityToTableNameMap.get(entityClazz);

                Object instance = tableToRepositoryMap.get(tableName);

                proxies.add(instance);
            }

            // add to repository class map
            repositoryClassMap.put( clazzName, proxies );
        }
        return repositoryClassMap;
    }

    private static Type[] getPkAndEntityTypesFromRepositoryClazz(Class<?> repositoryClazz){
        return ((ParameterizedType) repositoryClazz.getGenericInterfaces()[0]).getActualTypeArguments();
    }

    public static Map<String, Table> loadCatalog(Map<String, VmsDataModel> vmsDataModelMap, Map<Class<?>, String> entityToTableNameMap) throws NoSuchFieldException {

        Map<String, Table> catalog = new HashMap<>(vmsDataModelMap.size());
        Map<VmsDataModel, Tuple<Schema, Map<String, int[]>>> dataSchemaToPkMap = new HashMap<>(vmsDataModelMap.size());

        /*
         * Build primary key index and map the foreign keys (internal to this VMS)
         */
        for (var entry : entityToTableNameMap.entrySet()) {

            VmsDataModel vmsDataModel = vmsDataModelMap.get( entry.getValue() );

            // check if autogenerated annotation is present. a reverse index would be nice here...
            Class<?> entityClazz = entry.getKey();
            boolean generated = false;
            if(vmsDataModel.primaryKeyColumns.length == 1){
                // get primary key name
                int pos = vmsDataModel.primaryKeyColumns[0];
                String pkColumn = vmsDataModel.columnNames[pos];
                generated = entityClazz.getDeclaredField(pkColumn).getAnnotation((GeneratedValue.class)) != null;
            }

            final Schema schema = new Schema(vmsDataModel.columnNames, vmsDataModel.columnDataTypes,
                    vmsDataModel.primaryKeyColumns, vmsDataModel.constraintReferences, generated);

            if(vmsDataModel.foreignKeyReferences != null && vmsDataModel.foreignKeyReferences.length > 0){
                // build
                Map<String, List<ForeignKeyReference>> fksPerTable = Stream.of( vmsDataModel.foreignKeyReferences )
                                .collect( Collectors.groupingBy(ForeignKeyReference::vmsTableName ) );

                // table name, fields
                Map<String, int[]> definitiveMap = buildSchemaForeignKeyMap(vmsDataModel, fksPerTable, vmsDataModelMap);

                dataSchemaToPkMap.put(vmsDataModel, Tuple.of(schema, definitiveMap));
            } else {
                dataSchemaToPkMap.put(vmsDataModel, Tuple.of(schema, null));
            }

        }

        Map<String, PrimaryIndex> vmsDataSchemaToIndexMap = new HashMap<>(dataSchemaToPkMap.size());

        Map<String, List<AbstractIndex<IKey>>> vmsDataSchemaToSecondaryIndexMap = new HashMap<>();

        // mount vms data schema to consistent index map
        for (var entry : dataSchemaToPkMap.entrySet()) {

            Schema schema = entry.getValue().t1();

            PrimaryIndex consistentIndex = createPrimaryIndex(entry.getKey().tableName, schema);

            vmsDataSchemaToIndexMap.put( entry.getKey().tableName, consistentIndex );

            // secondary indexes
            if(entry.getValue().t2() != null) {
                List<AbstractIndex<IKey>> listSecIndexes = new ArrayList<>(entry.getValue().t2().size());
                vmsDataSchemaToSecondaryIndexMap.put(entry.getKey().tableName, listSecIndexes);

                // now create the secondary index (a - based on foreign keys and b - based on non-foreign keys)
                for (var secIdx : entry.getValue().t2().entrySet()) {
                    AbstractIndex<IKey> nuhi = createNonUniqueIndex(schema, secIdx.getValue(),
                            entry.getKey().tableName + "_" + secIdx.getKey());
                    listSecIndexes.add(nuhi);
                }
            }

        }

        // now I have the pk indexes and the fks
        for (var entry : dataSchemaToPkMap.entrySet()) {

            VmsDataModel vmsDataSchema = entry.getKey();
            Tuple<Schema, Map<String, int[]>> tupleSchemaFKs = entry.getValue();

            PrimaryIndex primaryIndex = vmsDataSchemaToIndexMap.get( vmsDataSchema.tableName );
            Table table;

            if(entry.getValue().t2() != null) {

                Map<PrimaryIndex, int[]> fks = new HashMap<>(tupleSchemaFKs.t2().size());

                // build fks
                for (var fk : tupleSchemaFKs.t2().entrySet()) {
                    fks.put(vmsDataSchemaToIndexMap.get(fk.getKey()), fk.getValue());
                }

                Map<IIndexKey, NonUniqueSecondaryIndex> secIndexMap = new HashMap<>();

                // build secondary indexes (for foreign keys)
                for (AbstractIndex<IKey> idx : vmsDataSchemaToSecondaryIndexMap.get(vmsDataSchema.tableName)) {
                    secIndexMap.put(idx.key(), new NonUniqueSecondaryIndex(primaryIndex, idx));
                }
                table = new Table(vmsDataSchema.tableName, tupleSchemaFKs.t1(), primaryIndex, fks, secIndexMap);
            } else {
                table = new Table(vmsDataSchema.tableName, tupleSchemaFKs.t1(), primaryIndex);
            }

            catalog.put( vmsDataSchema.tableName, table );
        }

        return catalog;
    }

    private static AbstractIndex<IKey> createNonUniqueIndex(Schema schema, int[] columnsIndex, String fileName){
        if(IN_MEMORY_STORAGE){
            // return new PrimaryIndex(new UniqueHashMapIndex(schema));
            // TODO fix this later
            return new NonUniqueHashMapIndex(schema);
        } else {
            OrderedRecordBuffer[] buffers = loadOrderedBuffers(MemoryUtils.DEFAULT_NUM_BUCKETS, MemoryUtils.DEFAULT_PAGE_SIZE, fileName);
            return new NonUniqueHashIndex(buffers, schema, columnsIndex);
        }
    }

    private static OrderedRecordBuffer[] loadOrderedBuffers(int numBuckets, int bucketSize, String fileName){

        long sizeInBytes = (long) numBuckets * bucketSize;

        Cleaner cleaner = Cleaner.create();
        ResourceScope scope = ResourceScope.newSharedScope(cleaner);
        MemorySegment segment;
        try {
            segment = mapFileIntoMemorySegment(sizeInBytes, fileName);
        } catch (Exception e){
            segment = MemorySegment.allocateNative(sizeInBytes, scope);
        }

        long address = segment.address().toRawLongValue();

        OrderedRecordBuffer[] buffers = new OrderedRecordBuffer[numBuckets];

        for(int i = 0; i < numBuckets; i++){
            buffers[i] = loadOrderedRecordBuffer(address, bucketSize);
            address = address + bucketSize;
        }

        return buffers;

    }

    private static PrimaryIndex createPrimaryIndex(String fileName, Schema schema) {
        if(IN_MEMORY_STORAGE){
            if(schema.isPrimaryKeyAutoGenerated()){
                return new PrimaryIndex(new UniqueHashMapIndex(schema), new IntegerPrimaryKeyGenerator());
            }
            return new PrimaryIndex(new UniqueHashMapIndex(schema));
        } else {
            // map this to a file, so whenever a batch commit event arrives, it can trigger logging the entire file
            RecordBufferContext recordBufferContext = loadRecordBuffer(10, schema.getRecordSize(), fileName);
            UniqueHashBufferIndex pkIndex = new UniqueHashBufferIndex(recordBufferContext, schema);
            return new PrimaryIndex(pkIndex);
        }

    }

    private static Map<String, int[]> buildSchemaForeignKeyMap(VmsDataModel dataSchemaToBuild, Map<String, List<ForeignKeyReference>> fksPerTable, Map<String, VmsDataModel> dataSchemaMap) {
        Map<String, int[]> res = new HashMap<>();
        for( var entry : fksPerTable.entrySet() ){
            int[] intArray = new int[ entry.getValue().size() ];
            int i = 0;
            // get parent data schema
            VmsDataModel dataSchema = dataSchemaMap.get( entry.getKey() );
            // first check if the foreign keys defined actually map to a column in parent table
            for(var fkColumn : entry.getValue()){
                if(dataSchema.findColumnPosition(fkColumn.columnName()) == -1) {
                    throw new RuntimeException("Cannot find foreign key " + fkColumn + " that refers to a PK in parent table: " + entry.getKey());
                }
                intArray[i] = dataSchemaToBuild.findColumnPosition(fkColumn.columnName());
                i++;
            }
            res.put( dataSchema.tableName, intArray );
        }
        return res;
    }

    private static OrderedRecordBuffer loadOrderedRecordBuffer(long address, int size){
        AppendOnlyBuffer appendOnlyBuffer = new AppendOnlyBuffer(address, size);
        return new OrderedRecordBuffer(appendOnlyBuffer);
    }

    private static RecordBufferContext loadRecordBuffer(int maxNumberOfRecords, int recordSize, String append){

        Cleaner cleaner = Cleaner.create();
        ResourceScope scope = ResourceScope.newSharedScope(cleaner);
        long sizeInBytes = (long) maxNumberOfRecords * recordSize;
        try {
            MemorySegment segment = mapFileIntoMemorySegment(sizeInBytes, append);
            return new RecordBufferContext(segment, maxNumberOfRecords);
        } catch (Exception e){
            logger.warning("Could not map file. Resorting to direct memory allocation attempt: "+e.getMessage());
            MemorySegment segment = MemorySegment.allocateNative(sizeInBytes, scope);
            return new RecordBufferContext(segment, maxNumberOfRecords);
        }

    }

    private static MemorySegment mapFileIntoMemorySegment(long bytes, String append) {

        String userHome = System.getProperty("user.home");

        if(userHome == null){
            logger.warning("User home directory is not set in the environment. Resorting to /usr/local/lib");
            userHome = "/usr/local/lib";
        }

        String filePath = userHome + "/vms/" + append;

        logger.info("Attempt to delete existing file in directory: "+filePath);

        File file = new File(filePath);
        if (file.exists()) {
            if(!file.delete()) throw new IllegalStateException("File can not be deleted");
        }

        logger.info("Attempt to create new file in directory: "+filePath);

        if(file.getParentFile().mkdirs()){
            logger.info("Parent directory required being created.");
        } else {
            logger.info("Parent directory does not need to be created.");
        }

        try {

            if(file.createNewFile()) {
                logger.info("Attempt to create new file in directory: "+filePath+" completed successfully.");
                return MemorySegment.mapFile(
                        file.toPath(),
                        0,
                        bytes,
                        FileChannel.MapMode.READ_WRITE,
                        ResourceScope.newSharedScope());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        throw new IllegalStateException("File could not be created");

    }

}
