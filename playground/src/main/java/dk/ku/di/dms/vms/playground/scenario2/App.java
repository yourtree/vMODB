package dk.ku.di.dms.vms.playground.scenario2;

import dk.ku.di.dms.vms.coordinator.server.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.server.coordinator.options.BatchReplicationStrategy;
import dk.ku.di.dms.vms.coordinator.server.coordinator.options.CoordinatorOptions;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.schema.VmsEventSchema;
import dk.ku.di.dms.vms.modb.common.schema.network.NetworkNode;
import dk.ku.di.dms.vms.modb.common.schema.network.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.VmsIdentifier;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.playground.app.EventExample;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.facade.ModbModules;
import dk.ku.di.dms.vms.sdk.embed.handler.EmbedVmsEventHandler;
import dk.ku.di.dms.vms.sdk.embed.metadata.EmbedMetadataLoader;
import dk.ku.di.dms.vms.sdk.embed.scheduler.EmbedVmsTransactionScheduler;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Logger;

/**
 *
 * Scenario 2: Two VMSs and a coordinator.
 * Two transactions:
 * a. Run in the vms 1
 * b. Start at vms 1 and finishes at vms 2
 *
 * That means vms 2 may receive not sequential tid.
 * Keeping track of the previous tid in the scheduler must occur.
 *
 * "Unix-based systems declare ports below 1024 as privileged"
 * https://stackoverflow.com/questions/25544849/java-net-bindexception-permission-denied-when-creating-a-serversocket-on-mac-os
 */
public class App 
{

    protected static final Logger logger = Logger.getLogger("App");

    private static final String transactionName = "tx_example";

    // input transactions
    private static final BlockingQueue<TransactionInput> parsedTransactionRequests = new LinkedBlockingDeque<>();

    public static void main( String[] args ) throws IOException, NoSuchFieldException, IllegalAccessException {

        // the reflections is scanning all the packages, not respecting the package passed
        List<String> inToDiscard = Collections.emptyList();
        List<String> outToDiscard = List.of("out3");
        List<String> inToSwap = List.of("out2");

        loadMicroservice( new NetworkNode("localhost", 1080),
                "example",
                "dk.ku.di.dms.vms.playground.app",
                inToDiscard,
                outToDiscard,
                inToSwap);

        inToSwap = inToDiscard;
        inToDiscard = List.of("in");
        outToDiscard = List.of("out");

        loadMicroservice(
                new NetworkNode("localhost", 1081),
                "example2",
                "dk.ku.di.dms.vms.playground.scenario2",
                inToDiscard,
                outToDiscard,
                inToSwap);

        loadCoordinator();

        Thread producerThread = new Thread(new Producer());
        producerThread.start();

    }

    private static class Producer implements Runnable {

        @Override
        public void run() {

            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

            int val = 1;

            while(val < 3) {

                EventExample eventExample = new EventExample(val);

                String payload = serdes.serialize(eventExample, EventExample.class);

                TransactionInput.Event eventPayload = new TransactionInput.Event("in", payload);

                TransactionInput txInput = new TransactionInput(transactionName, eventPayload);

                logger.info("[Producer] Adding "+val);

                parsedTransactionRequests.add(txInput);

                try {
                    //logger.info("Producer going to bed... ");
                    Thread.sleep(10000);
                    //logger.info("Producer woke up! Time to insert one more ");
                } catch (InterruptedException ignored) { }

                val++;

            }

            logger.info("Producer going to bed definitely... ");
        }
    }

    private static void loadCoordinator() throws IOException {

        ServerIdentifier serverEm1 = new ServerIdentifier( "localhost", 1082 );
        ServerIdentifier serverEm2 = new ServerIdentifier( "localhost", 1083 );

        Map<Integer, ServerIdentifier> serverMap = new HashMap<>(2);
        serverMap.put(serverEm1.hashCode(), serverEm1);
        serverMap.put(serverEm2.hashCode(), serverEm2);

        ExecutorService socketPool = Executors.newFixedThreadPool(2);

        NetworkNode vms1 = new NetworkNode("localhost", 1080);
        NetworkNode vms2 = new NetworkNode("localhost", 1081);

        Map<Integer,NetworkNode> VMSs = new HashMap<>(2);
        VMSs.put(vms1.hashCode(), vms1);
        VMSs.put(vms2.hashCode(), vms2);

        TransactionBootstrap txBootstrap = new TransactionBootstrap();
        TransactionDAG dag =  txBootstrap.init(transactionName)
                .input( "a", "example", "in" )
                .terminal("b", "example", "a")
                .terminal("c", "example2", "a")
                .build();

        Map<String, TransactionDAG> transactionMap = new HashMap<>(1);
        transactionMap.put(dag.name, dag);

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( );

        Coordinator coordinator = new Coordinator(
                socketPool,
                serverMap,
                null,
                VMSs,
                transactionMap,
                serverEm1,
                new CoordinatorOptions(),
                0,
                1,
                BatchReplicationStrategy.NONE,
                App.parsedTransactionRequests,
                serdes
        );

        Thread coordinatorThread = new Thread(coordinator);
        coordinatorThread.start();

    }

    private static void loadMicroservice(NetworkNode node, String vmsName, String packageName, List<String> inToDiscard, List<String> outToDiscard, List<String> inToSwap) throws IOException, NoSuchFieldException, IllegalAccessException {

        VmsEmbedInternalChannels vmsInternalPubSubService = new VmsEmbedInternalChannels();

        VmsRuntimeMetadata vmsMetadata = EmbedMetadataLoader.loadRuntimeMetadata(packageName);

        // discard events
        for(String in : inToDiscard)
            vmsMetadata.inputEventSchema().remove(in);

        for(String out : outToDiscard)
            vmsMetadata.outputEventSchema().remove(out);

        for(String in : inToSwap) {
            VmsEventSchema eventSchema = vmsMetadata.inputEventSchema().remove(in);
            vmsMetadata.outputEventSchema().put(in, eventSchema);
        }

        ModbModules modbModules = EmbedMetadataLoader.loadModbModulesIntoRepositories(vmsMetadata);

        assert vmsMetadata != null;

        ExecutorService readTaskPool = Executors.newSingleThreadExecutor();

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        EmbedVmsTransactionScheduler scheduler =
                new EmbedVmsTransactionScheduler(
                        readTaskPool,
                        vmsInternalPubSubService,
                        vmsMetadata.queueToVmsTransactionMap(),
                        vmsMetadata.queueToEventMap(),
                        serdes,
                        modbModules.catalog());

        VmsIdentifier vmsIdentifier = new VmsIdentifier(
                node.host, node.port, vmsName,
                0, 0,
                vmsMetadata.dataSchema(),
                vmsMetadata.inputEventSchema(), vmsMetadata.outputEventSchema());

        ExecutorService socketPool = Executors.newFixedThreadPool(2);

        EmbedVmsEventHandler eventHandler = new EmbedVmsEventHandler(
                    vmsInternalPubSubService, vmsIdentifier, vmsMetadata, serdes, socketPool );

        Thread eventHandlerThread = new Thread(eventHandler);
        eventHandlerThread.start();

        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.start();

    }

}
