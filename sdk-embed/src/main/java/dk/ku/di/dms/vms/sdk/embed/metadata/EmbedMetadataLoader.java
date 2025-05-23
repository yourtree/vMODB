package dk.ku.di.dms.vms.sdk.embed.metadata;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.VmsIndex;
import dk.ku.di.dms.vms.modb.api.annotations.VmsPartialIndex;
import dk.ku.di.dms.vms.modb.common.constraint.ForeignKeyReference;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataModel;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.transaction.OperationalAPI;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.NonUniqueSecondaryIndex;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.PrimaryIndex;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.UniqueSecondaryIndex;
import dk.ku.di.dms.vms.modb.utils.StorageUtils;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadataLoader;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;

import javax.persistence.GeneratedValue;
import java.lang.reflect.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class EmbedMetadataLoader {

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

                ByteBuddy byteBuddy = new ByteBuddy();
                Class<?> type;
                try (DynamicType.Unloaded<?> dynamicType = byteBuddy
                        .subclass(generic, ConstructorStrategy.Default.IMITATE_SUPER_CLASS)
                        .implement(repositoryType)
                        .method(ElementMatchers.isAnnotatedWith(Query.class) )
                        .intercept(
                                MethodDelegation.to(AbstractProxyRepository.Interceptor.class)
                        )
                        .name(repositoryType.getSimpleName().replaceFirst("I","") + "Impl")
                        .make()) {
                    type = dynamicType
                            .load(ClassLoader.getSystemClassLoader(), ClassLoadingStrategy.Default.INJECTION)
                            .getLoaded();
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

    private record IndexMetadata(Integer columnPos, String indexName){}
    private record PartialIndexMetadata(Integer columnPos, String indexName, Object value){}

    private record SchemaMapping(
            Schema schema,
            // key: table name value: columns
            Map<String, Tuple<int[],int[]>> secondaryIndexMap,
            List<IndexMetadata> indexMetadataList,
            // value: column, value
            List<PartialIndexMetadata> partialIndexMetadataList){}

    public static Map<String, Table> loadCatalog(Map<String, VmsDataModel> vmsDataModelMap,
                                                 Map<Class<?>, String> entityToTableNameMap,
                                                 boolean isCheckpointing,
                                                 boolean isTruncating,
                                                 int maxRecords) throws NoSuchFieldException {
        Map<String, Table> catalog = new HashMap<>(vmsDataModelMap.size());
        Map<VmsDataModel, SchemaMapping> dataSchemaToPkMap = new HashMap<>(vmsDataModelMap.size());

        // Build primary key index and map the foreign keys (internal to this VMS)
        for (var entry : entityToTableNameMap.entrySet()) {
            VmsDataModel vmsDataModel = vmsDataModelMap.get( entry.getValue() );

            // check if autogenerated annotation is present. a reverse index would be nice here...
            Class<?> entityClazz = entry.getKey();
            final Schema schema = buildEntitySchema(vmsDataModel, entityClazz);

            // indexes
            final List<Field> indexList = Arrays.stream(entityClazz.getFields()).filter(f->f.getAnnotation(VmsIndex.class)!=null).toList();
            List<IndexMetadata> indexMetadataList = new ArrayList<>();
            for(Field field : indexList){
                VmsIndex ann = field.getAnnotation(VmsIndex.class);
                Integer pos = vmsDataModel.findColumnPosition(field.getName());
                indexMetadataList.add( new IndexMetadata(pos, ann.name()) );
            }

            // partial indexes
            final List<Field> partialIndexList = Arrays.stream(entityClazz.getFields()).filter(f->f.getAnnotation(VmsPartialIndex.class)!=null).toList();
            List<PartialIndexMetadata> partialIndexMetadataList = new ArrayList<>();
            for(Field field : partialIndexList){
                VmsPartialIndex ann = field.getAnnotation(VmsPartialIndex.class);
                Integer pos = vmsDataModel.findColumnPosition(field.getName());
                partialIndexMetadataList.add( new PartialIndexMetadata(pos, ann.name(), ann.value()) );
            }

            // indexes for foreign keys
            if(vmsDataModel.foreignKeyReferences != null && vmsDataModel.foreignKeyReferences.length > 0){
                // build
                Map<String, List<ForeignKeyReference>> fksPerTable = Stream.of( vmsDataModel.foreignKeyReferences )
                                .collect( Collectors.groupingBy(ForeignKeyReference::parentTableName) );
                // table name, fields
                Map<String, Tuple<int[],int[]>> secondaryIndexMap = buildSchemaForeignKeyMap(vmsDataModel, fksPerTable, vmsDataModelMap);
                dataSchemaToPkMap.put(vmsDataModel, new SchemaMapping(schema, secondaryIndexMap, indexMetadataList, partialIndexMetadataList));
            } else {
                dataSchemaToPkMap.put(vmsDataModel, new SchemaMapping(schema, Map.of(), indexMetadataList, partialIndexMetadataList));
            }
        }

        Map<String, PrimaryIndex> tableToPrimaryIndexMap = new HashMap<>(dataSchemaToPkMap.size());
        Map<String, List<ReadWriteIndex<IKey>>> tableToSecondaryIndexMap = new HashMap<>();
        Map<String, List<ReadWriteIndex<IKey>>> tableToPartialIndexMap = new HashMap<>();

        // partial indexes metadata
        Map<IIndexKey, Tuple<Integer, Object>> partialIndexMetaMap = new HashMap<>();

        Properties properties = ConfigUtils.loadProperties();

        // mount vms data schema to consistent index map
        for (var entry : dataSchemaToPkMap.entrySet()) {

            Schema schema = entry.getValue().schema();

            String tableName = entry.getKey().tableName;

            // overwrite max records if necessary
            int maxRecords_ = maxRecords;
            String numRec = properties.getProperty("max_records."+tableName);
            if(numRec != null && !numRec.isBlank()){
                maxRecords_ = Integer.parseInt(numRec);
            }

            boolean chaining = false;
            if(properties.getProperty("table."+tableName+".chaining") != null){
                chaining = Boolean.parseBoolean( properties.getProperty("table."+tableName+".chaining") );
            }

            PrimaryIndex primaryIndex = StorageUtils.createPrimaryIndex(tableName, schema, isCheckpointing, isTruncating, chaining, maxRecords_);
            tableToPrimaryIndexMap.put(tableName, primaryIndex);

            // normal indexes (i.e., non-partial) and foreign key indexes go here?
            List<ReadWriteIndex<IKey>> listSecondaryIndexes = new ArrayList<>();
            tableToSecondaryIndexMap.put(tableName, listSecondaryIndexes);

            List<ReadWriteIndex<IKey>> listPartialIndexes = new ArrayList<>();
            tableToPartialIndexMap.put(tableName, listPartialIndexes);

            // secondary indexes based on foreign keys
            if(!entry.getValue().secondaryIndexMap().isEmpty()) {
                // now create the secondary index (a - based on foreign keys and b - based on non-foreign keys)
                for (var secIdx : entry.getValue().secondaryIndexMap().entrySet()) {
                    ReadWriteIndex<IKey> nuhi = StorageUtils.createNonUniqueIndex(schema, secIdx.getValue().t1(), "FK_"+secIdx.getKey() );
                    listSecondaryIndexes.add(nuhi);
                }
            }

            if(!entry.getValue().indexMetadataList().isEmpty()) {
                Map<String, List<IndexMetadata>> indexMetadataByName = entry.getValue().indexMetadataList().stream()
                        .collect(Collectors.groupingBy(IndexMetadata::indexName));
                for (var idxEntry : indexMetadataByName.entrySet()) {
                    ReadWriteIndex<IKey> nuhi;
                    if(idxEntry.getValue().size() == 1) {
                        nuhi = StorageUtils.createNonUniqueIndex(schema, new int[]{idxEntry.getValue().getFirst().columnPos()}, idxEntry.getKey());
                    } else {
                        int[] columnList = idxEntry.getValue().stream().mapToInt(c-> c.columnPos).toArray();
                        nuhi = StorageUtils.createNonUniqueIndex(schema, columnList, idxEntry.getKey() );
                    }
                    listSecondaryIndexes.add(nuhi);
                }
            }

            // secondary indexes based on annotation
            if(!entry.getValue().partialIndexMetadataList().isEmpty()) {
                for (PartialIndexMetadata partialIdx : entry.getValue().partialIndexMetadataList()) {
                    // not all partial indexes are unique.... how is it working?
                    ReadWriteIndex<IKey> uniquePartialIndex = StorageUtils.createUniqueIndex(schema, new int[]{ partialIdx.columnPos() }, partialIdx.indexName() );
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
            Map<PrimaryIndex, int[]> foreignKeysMap = new HashMap<>();
            for (var fk : schemaMapping.secondaryIndexMap().entrySet()) {
                // cannot send the parent table columns. must scan the own vms data model column positions
                foreignKeysMap.put(tableToPrimaryIndexMap.get(fk.getKey()), fk.getValue().t2());
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
                    schemaMapping.schema(),
                    primaryIndex,
                    foreignKeysMap,
                    secondaryIndexMap,
                    partialIndexMetaMap,
                    partialIndexMap);

            catalog.put( vmsDataSchema.tableName, table );
        }
        return catalog;
    }

    public static Schema buildEntitySchema(VmsDataModel vmsDataModel, Class<?> entityClazz) throws NoSuchFieldException {
        boolean generated = false;
        if(vmsDataModel.primaryKeyColumns.length == 1){
            // get primary key name
            int pos = vmsDataModel.primaryKeyColumns[0];
            String pkColumn = vmsDataModel.columnNames[pos];
            generated = entityClazz.getDeclaredField(pkColumn).getAnnotation((GeneratedValue.class)) != null;
        }
        return new Schema(vmsDataModel.columnNames, vmsDataModel.columnDataTypes,
                vmsDataModel.primaryKeyColumns, vmsDataModel.constraintReferences, generated);
    }

    private static Map<String, Tuple<int[],int[]>> buildSchemaForeignKeyMap(VmsDataModel childDataModel,
            Map<String, List<ForeignKeyReference>> fksPerTable, Map<String, VmsDataModel> dataModelMap) {
        Map<String, Tuple<int[],int[]>> res = new HashMap<>();
        for( var entry : fksPerTable.entrySet() ){
            int[] parentColumns = new int[ entry.getValue().size() ];
            int[] childColumns = new int[ entry.getValue().size() ];
            int i = 0;
            // get parent data schema
            VmsDataModel parentDataModel = dataModelMap.get( entry.getKey() );
            // first check if the foreign keys defined actually map to a column in parent table
            for(ForeignKeyReference fkColumn : entry.getValue()){
                parentColumns[i] = parentDataModel.findColumnPosition(fkColumn.parentColumnName());
                if(parentColumns[i] == -1) {
                    throw new RuntimeException("Cannot find foreign key " + fkColumn + " that refers to a PK in parent table: " + entry.getKey());
                }
                childColumns[i] = childDataModel.findColumnPosition(fkColumn.localColumnName());
                if(childColumns[i] == -1) {
                    throw new RuntimeException("Cannot find column name " + fkColumn.localColumnName() + " that refers to a column in child table: " + childDataModel.tableName);
                }
                i++;
            }
            res.put( parentDataModel.tableName, new Tuple<>(parentColumns, childColumns) );
        }
        return res;
    }

}
