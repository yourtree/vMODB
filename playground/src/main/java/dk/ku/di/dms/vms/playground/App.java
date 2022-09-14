package dk.ku.di.dms.vms.playground;

import dk.ku.di.dms.vms.coordinator.server.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.server.coordinator.options.BatchReplicationStrategy;
import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.schema.network.NetworkNode;
import dk.ku.di.dms.vms.modb.common.schema.network.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.VmsIdentifier;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.sdk.core.event.channel.IVmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.event.channel.VmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadataLoader;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionScheduler;
import dk.ku.di.dms.vms.sdk.embed.EmbedVmsEventHandler;
import dk.ku.di.dms.vms.sdk.embed.facade.EmbedRepositoryFacade;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Hello world!
 *
 */
public class App 
{


    public static void main( String[] args )
    {





    }

    private void loadCoordinator() throws IOException {

        ServerIdentifier serverEm1 = new ServerIdentifier( "localhost", 81 );
        ServerIdentifier serverEm2 = new ServerIdentifier( "localhost", 82 );

        Map<Integer, ServerIdentifier> serverMap = new HashMap<>(2);
        serverMap.put(serverEm1.hashCode(), serverEm1);
        serverMap.put(serverEm2.hashCode(), serverEm2);

        ExecutorService socketPool = Executors.newFixedThreadPool(2);

        NetworkNode vms = new NetworkNode("localhost", 80);

        Map<Integer,NetworkNode> VMSs = new HashMap<>(1);
        VMSs.put(vms.hashCode(), vms);

        TransactionBootstrap txBootstrap = new TransactionBootstrap();
        TransactionDAG dag =  txBootstrap.init("example")
                .input( "a", "example", "in" )
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>(1);
        transactionMap.put("example", dag);

        BlockingQueue<TransactionInput> parsedTransactionRequests = new LinkedBlockingDeque<>();

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

        Coordinator coordinator = new Coordinator(
                socketPool,
                serverMap,
                null,
                VMSs,
                transactionMap,
                new HashMap<>(),
                serverEm1,
                new CoordinatorOptions(),
                1,
                1,
                BatchReplicationStrategy.NONE,
                parsedTransactionRequests,
                serdes
        );

        // setup input of transaction

    }

    /**
     * Load one microservice at first and perform several transactions and batch commit
     */
    private void loadMicroservice() {

        IVmsInternalChannels vmsInternalPubSubService = VmsInternalChannels.getInstance();

        Constructor<?> constructor = EmbedRepositoryFacade.class.getConstructors()[0];

        VmsRuntimeMetadata vmsMetadata;
        try {
            vmsMetadata = VmsMetadataLoader.load(null, constructor);
        } catch (ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Cannot start VMs, error loading metadata.");
        }

        ExecutorService vmsAppLogicTaskPool = Executors.newSingleThreadExecutor();

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

        VmsTransactionScheduler scheduler =
                new VmsTransactionScheduler(vmsAppLogicTaskPool, vmsInternalPubSubService, vmsMetadata.eventToVmsTransactionMap());

        VmsIdentifier vmsIdentifier = new VmsIdentifier(
                "localhost", 80, "example",
                0, 0,
                vmsMetadata.vmsDataSchema(), vmsMetadata.vmsEventSchema());

        ExecutorService socketPool = Executors.newFixedThreadPool(2);

        EmbedVmsEventHandler eventHandler;

        try{
            eventHandler = new EmbedVmsEventHandler(
                    vmsInternalPubSubService, vmsIdentifier, vmsMetadata, serdes, socketPool );
        } catch (IOException e) {
            throw new RuntimeException("Cannot start VMs, error loading metadata.");
        }



    }

}
