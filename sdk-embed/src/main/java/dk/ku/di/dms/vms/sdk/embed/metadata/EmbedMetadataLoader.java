package dk.ku.di.dms.vms.sdk.embed.metadata;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataSchema;
import dk.ku.di.dms.vms.modb.definition.Catalog;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.modb.query.planner.Planner;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;
import dk.ku.di.dms.vms.sdk.core.facade.IVmsRepositoryFacade;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadataLoader;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.embed.facade.EmbedRepositoryFacade;
import dk.ku.di.dms.vms.sdk.embed.facade.ModbModules;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;

import java.lang.ref.Cleaner;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

public class EmbedMetadataLoader {

    public static VmsRuntimeMetadata load(String packageName) {

        try {

            @SuppressWarnings("unchecked")
            Constructor<IVmsRepositoryFacade> constructor = (Constructor<IVmsRepositoryFacade>) EmbedRepositoryFacade.class.getConstructors()[0];

            VmsRuntimeMetadata vmsRuntimeMetadata = VmsMetadataLoader.load(packageName, constructor);

            ModbModules modbModules = loadModbModules(vmsRuntimeMetadata);

            for(IVmsRepositoryFacade facade : vmsRuntimeMetadata.repositoryFacades()){
                ((EmbedRepositoryFacade)facade).setModbModules(modbModules);
            }

            return vmsRuntimeMetadata;

        } catch (ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            // logger.wa("Cannot start VMs, error loading metadata.");
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }

        return null;

    }

    private static ModbModules loadModbModules(VmsRuntimeMetadata vmsRuntimeMetadata){

        Catalog catalog = loadCatalog(vmsRuntimeMetadata);

        Analyzer analyzer = new Analyzer(catalog);
        Planner planner = new Planner();

        return new ModbModules(vmsRuntimeMetadata, catalog, analyzer, planner);
    }

    private static Catalog loadCatalog(VmsRuntimeMetadata vmsRuntimeMetadata) {

        Catalog catalog = new Catalog();

        for (VmsDataSchema vmsDataSchema : vmsRuntimeMetadata.dataSchema().values()) {

            Schema schema = new Schema(vmsDataSchema.columnNames, vmsDataSchema.columnDataTypes,
                    vmsDataSchema.primaryKeyColumns, null);

            RecordBufferContext recordBufferContext = loadMemoryBuffer(10, schema.getRecordSize());

            UniqueHashIndex pkIndex = new UniqueHashIndex(recordBufferContext, schema, schema.getPrimaryKeyColumns());

            Table table = new Table(vmsDataSchema.tableName, schema, pkIndex);

            catalog.insertTable(table);

        }

        return catalog;
    }

    private static RecordBufferContext loadMemoryBuffer(int maxNumberOfRecords, int recordSize){

        Cleaner cleaner = Cleaner.create();

        try(ResourceScope scope = ResourceScope.newSharedScope(cleaner)) {
            MemorySegment segment = MemorySegment.allocateNative(1024 * 100, scope);
            return new RecordBufferContext(segment, maxNumberOfRecords, recordSize);
        } catch (Exception e){

            ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer(1024 * 100);
            long address = MemoryUtils.getByteBufferAddress(buffer);

            return new RecordBufferContext(buffer, address, maxNumberOfRecords, recordSize);
        }

    }

}
