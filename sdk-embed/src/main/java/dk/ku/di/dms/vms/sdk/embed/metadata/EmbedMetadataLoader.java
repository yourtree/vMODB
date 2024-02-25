package dk.ku.di.dms.vms.sdk.embed.metadata;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.VmsPartialIndex;
import dk.ku.di.dms.vms.modb.common.constraint.ForeignKeyReference;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataModel;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashBufferIndex;
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
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.UniqueSecondaryIndex;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadataLoader;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;

import javax.persistence.GeneratedValue;
import java.io.File;
import java.io.IOException;
import java.lang.ref.Cleaner;
import java.lang.reflect.*;
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
                                                          OperationalAPI operationalAPI) throws InvocationTargetException, InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException {
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

                ByteBuddy byteBuddy = new ByteBuddy();
                Class<?> type;
                try (DynamicType.Unloaded<?> dynamicType = byteBuddy
                        .subclass(generic, ConstructorStrategy.Default.IMITATE_SUPER_CLASS)
                        .implement(repositoryType)
                         .method(ElementMatchers.isAnnotatedWith(Query.class) )
                        //.intercept(MethodCall.invoke( ElementMatchers.named("intercept")))
                        .intercept(
//                                ElementMatchers.definedMethod( Meth "intercept" )
                                MethodDelegation.to(AbstractProxyRepository.Interceptor.class)
//                                MethodCall.invoke( AbstractProxyRepository.class.getMethod(
//                                        "intercept", Object[].class ) ).withAllArguments()
////                                SuperMethodCall.INSTANCE.andThen(
////                                        MethodCall.invoke(
////                                                AbstractProxyRepository.class.getMethod( "intercept", Object[].class ) ) )
                        )
                        .name(repositoryType.getSimpleName().replaceFirst("I","") + "Impl")
                        .make()) {
                    type = dynamicType
                            .load(ClassLoader.getSystemClassLoader(), ClassLoadingStrategy.Default.INJECTION)
                            .getLoaded();

//                    type = byteBuddy.redefine(type)
//                            .method(ElementMatchers.isAnnotatedWith(Query.class) )
//                            .intercept( MethodCall.invoke(
//                                    AbstractProxyRepository.class.getMethod( "intercept" , Object[].class))
//                                    .onDefault()
//                                    .withAllArguments() );
                }

                Method[] queryMethods = repositoryType.getDeclaredMethods();
                // read queries
                var repositoryQueriesMap = VmsMetadataLoader.loadStaticQueries(queryMethods);

                Object instance = type.getConstructors()[0].newInstance(
                        pkClazz,
                        entityClazz,
                        catalog.get( tableName ),
                        operationalAPI,
                        repositoryQueriesMap
                );

                tableToRepositoryMap.put(tableName, instance);

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

    private record PartialIndexMetadata(Integer columnPos, String indexName, Object value){}

    private record SchemaMapping(
            Schema schema,
            // key: table name value: columns
            Map<String, int[]> secondaryIndexMap,
            // value: column, value
            List<PartialIndexMetadata> partialIndexMetadataList){}

    public static Map<String, Table> loadCatalog(Map<String, VmsDataModel> vmsDataModelMap,
                                                 Map<Class<?>, String> entityToTableNameMap) throws NoSuchFieldException {

        Map<String, Table> catalog = new HashMap<>(vmsDataModelMap.size());
        Map<VmsDataModel, SchemaMapping> dataSchemaToPkMap = new HashMap<>(vmsDataModelMap.size());

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

            // partial indexes
            final List<Field> partialIndexList = Arrays.stream(entityClazz.getFields()).filter(f->f.getAnnotation(VmsPartialIndex.class)!=null).toList();
            List<PartialIndexMetadata> partialIndexMetadataList = new ArrayList<>();
            for(Field field : partialIndexList){
                var ann = field.getAnnotation(VmsPartialIndex.class);
                Integer pos = vmsDataModel.findColumnPosition(field.getName());
                partialIndexMetadataList.add( new PartialIndexMetadata(pos, ann.name(), ann.value()) );
            }

            // indexes for foreign keys
            if(vmsDataModel.foreignKeyReferences != null && vmsDataModel.foreignKeyReferences.length > 0){
                // build
                Map<String, List<ForeignKeyReference>> fksPerTable = Stream.of( vmsDataModel.foreignKeyReferences )
                                .collect( Collectors.groupingBy(ForeignKeyReference::vmsTableName ) );

                // table name, fields
                Map<String, int[]> secondaryIndexMap = buildSchemaForeignKeyMap(fksPerTable, vmsDataModelMap);
                dataSchemaToPkMap.put(vmsDataModel, new SchemaMapping(schema, secondaryIndexMap, partialIndexMetadataList));
            } else {
                dataSchemaToPkMap.put(vmsDataModel, new SchemaMapping(schema, Map.of(), partialIndexMetadataList));
            }
        }

        Map<String, PrimaryIndex> tableToPrimaryIndexMap = new HashMap<>(dataSchemaToPkMap.size());
        Map<String, List<ReadWriteIndex<IKey>>> tableToSecondaryIndexMap = new HashMap<>();
        Map<String, List<ReadWriteIndex<IKey>>> tableToPartialIndexMap = new HashMap<>();

        // partial indexes metadata
        Map<IIndexKey, Tuple<Integer, Object>> partialIndexMetaMap = new HashMap<>();

        // mount vms data schema to consistent index map
        for (var entry : dataSchemaToPkMap.entrySet()) {

            Schema schema = entry.getValue().schema();
            PrimaryIndex consistentIndex = createPrimaryIndex(entry.getKey().tableName, schema);
            tableToPrimaryIndexMap.put(entry.getKey().tableName, consistentIndex);

            List<ReadWriteIndex<IKey>> listSecondaryIndexes = new ArrayList<>();
            tableToSecondaryIndexMap.put(entry.getKey().tableName, listSecondaryIndexes);

            List<ReadWriteIndex<IKey>> listPartialIndexes = new ArrayList<>();
            tableToPartialIndexMap.put(entry.getKey().tableName, listPartialIndexes);

            // secondary indexes based on foreign keys
            if(!entry.getValue().secondaryIndexMap().isEmpty()) {
                // now create the secondary index (a - based on foreign keys and b - based on non-foreign keys)
                for (var secIdx : entry.getValue().secondaryIndexMap().entrySet()) {
                    ReadWriteIndex<IKey> nuhi = createNonUniqueIndex(schema, secIdx.getValue(), "FK_"+secIdx.getKey() );
                    listSecondaryIndexes.add(nuhi);
                }
            }

            // secondary indexes based on annotation
            if(!entry.getValue().partialIndexMetadataList().isEmpty()) {
                for (var partialIdx : entry.getValue().partialIndexMetadataList()) {
                    ReadWriteIndex<IKey> uniquePartialIndex = createUniqueIndex(schema, new int[]{ partialIdx.columnPos() }, partialIdx.indexName() );
                    partialIndexMetaMap.put( uniquePartialIndex.key(), new Tuple<>(partialIdx.columnPos(), partialIdx.value() ) );
                    listPartialIndexes.add(uniquePartialIndex);
                }
            }
        }

        // now I have the primary key indexes, foreign-key indexes, and other secondary indexes
        // build the multi-versioning layer on top of them!
        for (var entry : dataSchemaToPkMap.entrySet()) {

            VmsDataModel vmsDataSchema = entry.getKey();
            SchemaMapping schemaMapping = entry.getValue();

            PrimaryIndex primaryIndex = tableToPrimaryIndexMap.get(vmsDataSchema.tableName);

            // build foreign key indexes metadata
            Map<PrimaryIndex, int[]> fks = new HashMap<>();
            for (var fk : schemaMapping.secondaryIndexMap().entrySet()) {
                fks.put(tableToPrimaryIndexMap.get(fk.getKey()), fk.getValue());
            }

            // build foreign key secondary indexes
            Map<IIndexKey, NonUniqueSecondaryIndex> secondaryIndexMap = new HashMap<>();
            for (ReadWriteIndex<IKey> idx : tableToSecondaryIndexMap.get(vmsDataSchema.tableName)) {
                secondaryIndexMap.put(idx.key(), new NonUniqueSecondaryIndex(primaryIndex, idx));
            }

            // build partial indexes
            Map<IIndexKey, UniqueSecondaryIndex> partialIndexMap = new HashMap<>();
            for(ReadWriteIndex<IKey> idx : tableToPartialIndexMap.get(vmsDataSchema.tableName)){
                partialIndexMap.put(idx.key(), new UniqueSecondaryIndex(primaryIndex));
            }

            Table table = new Table(vmsDataSchema.tableName,
                    schemaMapping.schema(), primaryIndex,
                    fks, secondaryIndexMap,
                    partialIndexMetaMap, partialIndexMap);

            catalog.put( vmsDataSchema.tableName, table );
        }

        return catalog;
    }

    private static ReadWriteIndex<IKey> createUniqueIndex(Schema schema, int[] columnsIndex, String indexName){
        if(IN_MEMORY_STORAGE){
            return new UniqueHashMapIndex(schema, columnsIndex);
        }
        RecordBufferContext recordBufferContext = loadRecordBuffer(10, schema.getRecordSize(), indexName);
        return new UniqueHashBufferIndex(recordBufferContext, schema);
    }

    private static ReadWriteIndex<IKey> createNonUniqueIndex(Schema schema, int[] columnsIndex, String indexName){
        if(IN_MEMORY_STORAGE){
            return new NonUniqueHashMapIndex(schema, columnsIndex);
        } else {
            OrderedRecordBuffer[] buffers = loadOrderedBuffers(MemoryUtils.DEFAULT_NUM_BUCKETS, MemoryUtils.DEFAULT_PAGE_SIZE, indexName);
            return new NonUniqueHashBufferIndex(buffers, schema, columnsIndex);
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
                return new PrimaryIndex(new UniqueHashMapIndex(schema, schema.getPrimaryKeyColumns()), new IntegerPrimaryKeyGenerator());
            }
            return new PrimaryIndex(new UniqueHashMapIndex(schema, schema.getPrimaryKeyColumns()));
        } else {
            // map this to a file, so whenever a batch commit event arrives, it can trigger logging the entire file
            RecordBufferContext recordBufferContext = loadRecordBuffer(10, schema.getRecordSize(), fileName);
            UniqueHashBufferIndex pkIndex = new UniqueHashBufferIndex(recordBufferContext, schema);
            return new PrimaryIndex(pkIndex);
        }
    }

    private static Map<String, int[]> buildSchemaForeignKeyMap(Map<String, List<ForeignKeyReference>> fksPerTable,
                                                               Map<String, VmsDataModel> dataModelMap) {
        Map<String, int[]> res = new HashMap<>();
        for( var entry : fksPerTable.entrySet() ){
            int[] intArray = new int[ entry.getValue().size() ];
            int i = 0;
            // get parent data schema
            VmsDataModel parentDataModel = dataModelMap.get( entry.getKey() );
            // first check if the foreign keys defined actually map to a column in parent table
            for(var fkColumn : entry.getValue()){
                intArray[i] = parentDataModel.findColumnPosition(fkColumn.columnName());
                if(intArray[i] == -1) {
                    throw new RuntimeException("Cannot find foreign key " + fkColumn + " that refers to a PK in parent table: " + entry.getKey());
                }
                i++;
            }
            res.put( parentDataModel.tableName, intArray );
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
