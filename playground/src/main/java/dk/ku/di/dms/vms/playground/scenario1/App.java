package dk.ku.di.dms.vms.playground.scenario1;

import dk.ku.di.dms.vms.coordinator.server.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.ConsumerVms;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.VmsIdentifier;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.transaction.TransactionFacade;
import dk.ku.di.dms.vms.playground.app.EventExample;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionScheduler;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.handler.EmbeddedVmsEventHandler;
import dk.ku.di.dms.vms.sdk.embed.metadata.EmbedMetadataLoader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Logger;

/**
 *
 * Scenario 1: One VMS and a coordinator. One transaction, running in the vms
 * -
 * "Unix-based systems declare ports below 1024 as privileged"
 * <a href="https://stackoverflow.com/questions/25544849/java-net-bindexception-permission-denied-when-creating-a-serversocket-on-mac-os">...</a>
 *
 */
public class App 
{

    protected static final Logger logger = Logger.getLogger("App");

    // input transactions
    private static final BlockingQueue<TransactionInput> parsedTransactionRequests = new LinkedBlockingDeque<>();

    public static void main( String[] args ) throws Exception {

        loadMicroservice();

        loadCoordinator();

        Thread producerThread = new Thread(new Producer());
        producerThread.start();

    }

    private static class Producer implements Runnable {

        @Override
        public void run() {

            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

            int val = 1;

            while(true) {

                if(val < 3) {

                    EventExample eventExample = new EventExample(val);

                    String payload = serdes.serialize(eventExample, EventExample.class);

                    TransactionInput.Event eventPayload = new TransactionInput.Event("in", payload);

                    TransactionInput txInput = new TransactionInput("example", eventPayload);

                    logger.info("[Producer] Adding " + val);

                    parsedTransactionRequests.add(txInput);

                }

                try {
                    //logger.info("Producer going to bed... ");
                    Thread.sleep(10000);
                    //logger.info("Producer woke up! Time to insert one more ");
                } catch (InterruptedException ignored) { }

                val++;

            }
        }
    }

    private static void loadCoordinator() throws IOException {

        ServerIdentifier serverEm1 = new ServerIdentifier( "localhost", 1081 );

        Map<Integer, ServerIdentifier> serverMap = new HashMap<>(2);
        serverMap.put(serverEm1.hashCode(), serverEm1);

        ExecutorService socketPool = Executors.newFixedThreadPool(2);

        ConsumerVms vms = new ConsumerVms("localhost", 1080);

        Map<Integer, ConsumerVms> VMSs = new HashMap<>(1);
        VMSs.put(vms.hashCode(), vms);

        TransactionDAG dag =  TransactionBootstrap.name("example")
                .input( "a", "example", "in" )
                // bad way to do it for single-microservice transactions
                .terminal("t", "example", "a")
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>(1);
        transactionMap.put("example", dag);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

        Coordinator coordinator = new Coordinator(
                serverMap,
                null,
                VMSs,
                transactionMap,
                serverEm1,
                new CoordinatorOptions(),
                1,
                1,
                App.parsedTransactionRequests,
                serdes
        );

        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

    }

    /**
     * Load one microservice at first and perform several transactions and batch commit
     */
    private static void loadMicroservice() throws Exception {

        VmsEmbedInternalChannels vmsInternalPubSubService = new VmsEmbedInternalChannels();

        VmsRuntimeMetadata vmsMetadata = EmbedMetadataLoader.loadRuntimeMetadata("dk.ku.di.dms.vms.playground.app");

        TransactionFacade transactionFacade = EmbedMetadataLoader.loadTransactionFacadeAndInjectIntoRepositories(vmsMetadata);

        if(vmsMetadata == null) throw new IllegalStateException("Cannot start VMs, error loading metadata.");

        ExecutorService vmsAppLogicTaskPool = Executors.newSingleThreadExecutor();

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

        VmsTransactionScheduler scheduler =
                new VmsTransactionScheduler(
                        vmsAppLogicTaskPool,
                        vmsInternalPubSubService,
                        vmsMetadata.queueToVmsTransactionMap(),
                        null);

        VmsIdentifier vmsIdentifier = new VmsIdentifier(
                "localhost", 1080, "example",
                0, 0,0,
                vmsMetadata.dataSchema(),
                vmsMetadata.inputEventSchema(), vmsMetadata.outputEventSchema());

        ExecutorService socketPool = Executors.newFixedThreadPool(2);

        EmbeddedVmsEventHandler eventHandler = EmbeddedVmsEventHandler.build(
                vmsIdentifier, null, null, null, vmsInternalPubSubService, vmsMetadata, serdes, socketPool );

        Thread eventHandlerThread = new Thread(eventHandler);
        eventHandlerThread.start();

        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.start();

    }

}
