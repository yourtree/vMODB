package dk.ku.di.dms.vms.sdk.embed.metadata;

import dk.ku.di.dms.vms.modb.common.constraint.ForeignKeyReference;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataSchema;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;
import dk.ku.di.dms.vms.modb.transaction.TransactionFacade;
import dk.ku.di.dms.vms.modb.transaction.multiversion.ConsistentIndex;
import dk.ku.di.dms.vms.sdk.core.facade.IVmsRepositoryFacade;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadataLoader;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.embed.facade.EmbedRepositoryFacade;
import dk.ku.di.dms.vms.sdk.embed.ingest.BulkDataLoader;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;

import java.io.File;
import java.io.IOException;
import java.lang.ref.Cleaner;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.logging.Logger.GLOBAL_LOGGER_NAME;
import static java.util.logging.Logger.getLogger;

public class EmbedMetadataLoader {

    private static final Logger logger = getLogger(GLOBAL_LOGGER_NAME);

    public static VmsRuntimeMetadata loadRuntimeMetadata(String... packages) {

        try {

            @SuppressWarnings("unchecked")
            Constructor<IVmsRepositoryFacade> constructor = (Constructor<IVmsRepositoryFacade>) EmbedRepositoryFacade.class.getConstructors()[0];

            return VmsMetadataLoader.load(packages, constructor);


        } catch (ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            logger.warning("Cannot start VMs, error loading metadata: "+e.getMessage());
        }

        return null;

    }

    public static TransactionFacade loadTransactionFacade(VmsRuntimeMetadata vmsRuntimeMetadata) {
        return loadTransactionFacade(vmsRuntimeMetadata, new HashSet<>());
    }

    public static TransactionFacade loadTransactionFacade(
            VmsRuntimeMetadata vmsRuntimeMetadata, Set<String> entitiesToExclude) {

        Map<String, Table> catalog = loadCatalog(vmsRuntimeMetadata, entitiesToExclude);

        TransactionFacade transactionFacade = TransactionFacade.build(catalog);

        for(Map.Entry<String, IVmsRepositoryFacade> facadeEntry : vmsRuntimeMetadata.repositoryFacades().entrySet()){
            ((EmbedRepositoryFacade)facadeEntry.getValue()).setDynamicDatabaseModules(transactionFacade, catalog.get(facadeEntry.getKey()));
        }

        // instantiate loader
        BulkDataLoader loader = new BulkDataLoader( vmsRuntimeMetadata.repositoryFacades(), vmsRuntimeMetadata.entityToTableNameMap(), VmsSerdesProxyBuilder.build() );

        vmsRuntimeMetadata.loadedVmsInstances().put("data_loader", loader);

        return transactionFacade;

    }

    private static Map<String, Table> loadCatalog(VmsRuntimeMetadata vmsRuntimeMetadata, Set<String> entitiesToExclude) {

        Map<String, Table> catalog = new HashMap<>(vmsRuntimeMetadata.dataSchema().size());
        Map<VmsDataSchema, Tuple<Schema, Map<String, int[]>>> dataSchemaToPkMap = new HashMap<>(vmsRuntimeMetadata.dataSchema().size());

        /*
         * Build primary key index and map the foreign keys (internal to this VMS)
         * TODO create secondary indexes. how do sec idx update see the updated states? must hold a reference to the PK...
         */
        for (VmsDataSchema vmsDataSchema : vmsRuntimeMetadata.dataSchema().values()) {

            if(!entitiesToExclude.contains(vmsDataSchema.tableName)) continue;

            final Schema schema = new Schema(vmsDataSchema.columnNames, vmsDataSchema.columnDataTypes,
                    vmsDataSchema.primaryKeyColumns, vmsDataSchema.constraintReferences);

            if(vmsDataSchema.foreignKeyReferences != null && vmsDataSchema.foreignKeyReferences.length > 0){
                // build
                Map<String, List<ForeignKeyReference>> res = Stream.of( vmsDataSchema.foreignKeyReferences )
                                .sorted( (x,y) -> schema.columnPosition( x.columnName() ) < schema.columnPosition( y.columnName() ) ? -1 : 1 )
                        .collect( Collectors.groupingBy(ForeignKeyReference::vmsTableName ) ); // Collectors.toUnmodifiableList() ) );

                Map<String, int[]> definitiveMap = buildSchemaForeignKeyMap( schema, vmsRuntimeMetadata.dataSchema(), res );

                dataSchemaToPkMap.put(vmsDataSchema, Tuple.of(schema, definitiveMap));

            } else {
                dataSchemaToPkMap.put(vmsDataSchema, Tuple.of(schema, null));
            }

        }

        Map<String, ConsistentIndex> vmsDataSchemaToIndexMap = new HashMap<>(dataSchemaToPkMap.size());

        // mount vms data schema to consistent index map
        for (var entry : dataSchemaToPkMap.entrySet()) {

            Schema schema = entry.getValue().t1();

            // map this to a file, so whenever a batch commit arrives i can make the file durable
            RecordBufferContext recordBufferContext = loadMemoryBuffer(10, schema.getRecordSize(), entry.getKey().tableName);

            UniqueHashIndex pkIndex = new UniqueHashIndex(recordBufferContext, schema);

            ConsistentIndex consistentIndex = new ConsistentIndex(pkIndex);

            vmsDataSchemaToIndexMap.put( entry.getKey().vmsName, consistentIndex );

        }

        // now I have the pk indexes and the fks
        for (var entry : dataSchemaToPkMap.entrySet()) {
            VmsDataSchema vmsDataSchema = entry.getKey();
            Tuple<Schema, Map<String, int[]>> tupleSchemaFKs = entry.getValue();

            Map<ConsistentIndex, int[]> fks = new HashMap<>();

            // build fks
            for(var fk : tupleSchemaFKs.t2().entrySet()){
                fks.put( vmsDataSchemaToIndexMap.get( fk.getKey() ), fk.getValue() );
            }

            Table table = new Table(vmsDataSchema.tableName, tupleSchemaFKs.t1(), vmsDataSchemaToIndexMap.get( vmsDataSchema.vmsName ), fks);
            catalog.put( vmsDataSchema.tableName, table );
        }

        return catalog;
    }

    private static Map<String, int[]> buildSchemaForeignKeyMap(Schema schema, Map<String, VmsDataSchema> dataSchemaMap, Map<String, List<ForeignKeyReference>> map) {

        Map<String, int[]> res = new HashMap<>();

        for( var entry : map.entrySet() ){

            VmsDataSchema dataSchema = dataSchemaMap.get( entry.getKey() );

            var list = entry.getValue();

            Integer[] aux = list.stream().map(p-> schema.columnPosition( p.columnName() ) ).toArray( Integer[]::new );

            int[] intArray = new int[aux.length];
            for(int i = 0; i < aux.length; i++){
                intArray[i] = aux[i];
            }

            res.put( dataSchema.vmsName, intArray );

        }

        return res;

    }

    private static RecordBufferContext loadMemoryBuffer(int maxNumberOfRecords, int recordSize, String append){

        Cleaner cleaner = Cleaner.create();
        ResourceScope scope = ResourceScope.newSharedScope(cleaner);
        long sizeInBytes = (long) maxNumberOfRecords * recordSize;
        try {

            MemorySegment segment = mapFileIntoMemorySegment(sizeInBytes, append);
            return new RecordBufferContext(segment, maxNumberOfRecords, recordSize);

        } catch (Exception e){

            logger.warning("Could not map file. Resorting to direct memory allocation attempt: "+e.getMessage());

            MemorySegment segment = MemorySegment.allocateNative(sizeInBytes, scope);
            return new RecordBufferContext(segment, maxNumberOfRecords, recordSize);

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
            logger.info("Parent directory don't need to be created.");
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
