package dk.ku.di.dms.vms.sdk.embed.ingest;

import dk.ku.di.dms.vms.modb.common.ByteUtils;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.sdk.core.facade.IVmsRepositoryFacade;
import dk.ku.di.dms.vms.sdk.embed.annotations.Loader;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.BATCH_OF_EVENTS;

/**
 * In the future we can define whether the bulk data load has transactional guarantees or not.
 */
@Loader("data_loader")
public class BulkDataLoader {

    private final Map<String, IVmsRepositoryFacade> repositoryFacades;
    private final Map<String, Class<?>> tableNameToEntityClazzMap;

    private final IVmsSerdesProxy serdes;

    public BulkDataLoader(Map<String, IVmsRepositoryFacade> repositoryFacades, Map<Class<?>, String> entityToTableNameMap, IVmsSerdesProxy serdes){
        this.repositoryFacades = repositoryFacades;

        this.tableNameToEntityClazzMap =
                entityToTableNameMap.entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

        this.serdes = serdes;
    }

    public void init(String tableName, ConnectionMetadata connectionMetadata){
        new BulkDataLoaderProtocol( tableNameToEntityClazzMap.get(tableName), repositoryFacades.get(tableName) ).init( connectionMetadata );
    }

    private class BulkDataLoaderProtocol {

        private final Class<?> type;
        private final IVmsRepositoryFacade repositoryFacade;

        protected BulkDataLoaderProtocol(Class<?> type, IVmsRepositoryFacade repositoryFacade){
            this.type = type;
            this.repositoryFacade = repositoryFacade;
        }

        public void init(ConnectionMetadata connectionMetadata){
            connectionMetadata.channel.read( connectionMetadata.readBuffer,
                    connectionMetadata, new ReadCompletionHandler() );
        }

        private class ReadCompletionHandler implements CompletionHandler<Integer, ConnectionMetadata> {

            @Override
            public void completed(Integer result, ConnectionMetadata connectionMetadata) {

                // should be a batch of events
                connectionMetadata.readBuffer.position(0);
                byte messageType = connectionMetadata.readBuffer.get();

                assert (messageType == BATCH_OF_EVENTS);

                int count = connectionMetadata.readBuffer.getInt();

                ByteBuffer bb = MemoryManager.getTemporaryDirectBuffer();
                ByteBuffer oldBB = connectionMetadata.readBuffer;
                connectionMetadata.readBuffer = bb;

                // set up read handler again without waiting for tx facade
                connectionMetadata.channel.read( connectionMetadata.readBuffer,
                        connectionMetadata, this );

                // TransactionFacade.bulkInsert(table, oldBB, count);
                // TODO call repository facade, it will convert the entity appropriately

                List<Object> entities = new ArrayList<>(count);
                for(int i = 0; i < count; i++){
                    int size = connectionMetadata.readBuffer.getInt();
                    String objStr = ByteUtils.extractStringFromByteBuffer(connectionMetadata.readBuffer, size);
                    Object object = serdes.deserialize( objStr, type );
                    entities.add(object);
                }

                repositoryFacade.insertAll( entities );

                oldBB.clear();
                MemoryManager.releaseTemporaryDirectBuffer(oldBB);

            }

            @Override
            public void failed(Throwable exc, ConnectionMetadata connectionMetadata) { }

        }

    }


}
