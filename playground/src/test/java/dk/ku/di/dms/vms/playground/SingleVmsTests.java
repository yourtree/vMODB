package dk.ku.di.dms.vms.playground;

import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.Coordinator;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.NetworkAddress;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.transaction.TransactionFacade;
import dk.ku.di.dms.vms.playground.app.EventExample;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionScheduler;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbeddedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.handler.EmbeddedVmsEventHandler;
import dk.ku.di.dms.vms.sdk.embed.metadata.EmbedMetadataLoader;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Logger;

import static java.lang.Thread.sleep;

/**
 * Unit test for tests involving a single VMS
 * {@link #batchOfThreeEventsTest()} Three transaction inputs are sent to be batched as part of a batch.
 * Another interesting test is sending many more events as part of a batch. The size must trespass a single message
 * and both the VMs and coordinator must handle that correctly.
 * Another test regards message delivery failures, including the batch and other events related to the batch protocol.
 */
public class SingleVmsTests
{
    protected static final Logger logger = Logger.getLogger("SingleVmsTests");

    private static final String transactionName = "example";

    // input transactions
    private final BlockingQueue<TransactionInput> parsedTransactionRequests = new LinkedBlockingDeque<>();

    @Test
    public void batchOfThreeEventsTest() throws Exception {

        // query microservice state afterwards and

        var vms = loadMicroservice();
        var coordinator = loadCoordinator();

        Thread producerThread = new Thread(new Producer());
        producerThread.start();

        sleep(10000);

        assert coordinator.getTid() == 4 && coordinator.getCurrentBatchOffset() == 2 && coordinator.getBatchOffsetPendingCommit() == 2;

        // query the coordinator batch to see if batch has evolved and how many batches has been committed

        coordinator.stop();
        vms.t1().stop();
        vms.t2().stop();

    }

    private class Producer implements Runnable {

        @Override
        public void run() {

            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

            int val = 1;

            while(val <= 3) {

                EventExample eventExample = new EventExample(val);

                String payload = serdes.serialize(eventExample, EventExample.class);

                TransactionInput.Event eventPayload = new TransactionInput.Event("in", payload);

                TransactionInput txInput = new TransactionInput(transactionName, eventPayload);

                logger.info("[Producer] Adding " + val);

                parsedTransactionRequests.add(txInput);

                val++;

            }
        }
    }

    private Coordinator loadCoordinator() throws IOException {

        ServerIdentifier serverEm1 = new ServerIdentifier( "localhost", 1081 );

        Map<Integer, ServerIdentifier> serverMap = new HashMap<>(2);
        serverMap.put(serverEm1.hashCode(), serverEm1);

        NetworkAddress vms = new NetworkAddress("localhost", 1080);

        Map<Integer, NetworkAddress> VMSs = new HashMap<>(1);
        VMSs.put(vms.hashCode(), vms);

        TransactionDAG dag =  TransactionBootstrap.name(transactionName)
                .input( "a", "example", "in" )
                // bad way to do it for single-microservice transactions
                .terminal("t", "example", "a")
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>(1);
        transactionMap.put(dag.name, dag);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

        Coordinator coordinator = Coordinator.buildDefault(
                serverMap,
                null,
                VMSs,
                transactionMap,
                serverEm1,
                new CoordinatorOptions().withBatchWindow(3000),
                1,
                1,
                parsedTransactionRequests,
                serdes
        );

        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

        return coordinator;
    }

    private Tuple<EmbeddedVmsEventHandler,VmsTransactionScheduler> loadMicroservice() throws Exception {

        VmsEmbeddedInternalChannels vmsInternalPubSubService = new VmsEmbeddedInternalChannels();

        VmsRuntimeMetadata vmsMetadata = EmbedMetadataLoader.loadRuntimeMetadata("dk.ku.di.dms.vms.playground.app");

        if(vmsMetadata == null) throw new IllegalStateException("Cannot start VMs, error loading metadata.");

        Map<String, Table> catalog = EmbedMetadataLoader.loadCatalog(vmsMetadata, Set.of() );

        TransactionFacade transactionFacade = EmbedMetadataLoader.loadTransactionFacadeAndInjectIntoRepositories(vmsMetadata, catalog);

        ExecutorService vmsAppLogicTaskPool = Executors.newSingleThreadExecutor();

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

        VmsTransactionScheduler scheduler =
                new VmsTransactionScheduler(
                        vmsAppLogicTaskPool,
                        vmsInternalPubSubService,
                        vmsMetadata.queueToVmsTransactionMap(),
                        null);

        VmsNode vmsIdentifier = new VmsNode(
                "localhost", 1080, "example",
                0, 0,0,
                vmsMetadata.dataSchema(),
                vmsMetadata.inputEventSchema(), vmsMetadata.outputEventSchema());

        ExecutorService socketPool = Executors.newFixedThreadPool(2);

        EmbeddedVmsEventHandler eventHandler = EmbeddedVmsEventHandler.buildWithDefaults(
                vmsIdentifier, null, transactionFacade, vmsInternalPubSubService, vmsMetadata, serdes, socketPool );

        Thread eventHandlerThread = new Thread(eventHandler);
        eventHandlerThread.start();

        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.start();

        return new Tuple<>(eventHandler,scheduler);
    }

}
