package dk.ku.di.dms.vms.sdk.embed.client;

import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.transaction.TransactionFacade;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionScheduler;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbeddedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.handler.EmbeddedVmsEventHandler;
import dk.ku.di.dms.vms.sdk.embed.metadata.EmbedMetadataLoader;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Starting point for initializing the runtime
 */
public final class VmsApplication {

    private static final Logger logger = Logger.getLogger("VmsApplication");

    public static void start(String host, int port, String[] packages, String... entitiesToExclude){

        // check first whether we are in decoupled or embed mode
        try {

            Optional<Package> optional = Arrays.stream(Package.getPackages()).filter(p ->
                             !p.getName().contains("dk.ku.di.dms.vms.sdk.embed")
                          && !p.getName().contains("dk.ku.di.dms.vms.sdk.core")
                          && !p.getName().contains("dk.ku.di.dms.vms.modb")
                          && !p.getName().contains("java")
                          && !p.getName().contains("sun")
                          && !p.getName().contains("jdk")
                          && !p.getName().contains("com")
                          && !p.getName().contains("org")
                                                        ).findFirst();

            String packageName = optional.map(Package::getName).orElse("Nothing");
            logger.info(packageName);

            if(packageName.equalsIgnoreCase("Nothing")) throw new IllegalStateException("Cannot identify package.");

            VmsEmbeddedInternalChannels vmsInternalPubSubService = new VmsEmbeddedInternalChannels();

            VmsRuntimeMetadata vmsMetadata = EmbedMetadataLoader.loadRuntimeMetadata(packages);

            Set<String> toExclude = entitiesToExclude != null ? Arrays.stream(entitiesToExclude).collect(
                    Collectors.toSet()) : new HashSet<>();

            TransactionFacade transactionFacade = EmbedMetadataLoader.loadTransactionFacadeAndInjectIntoRepositories(vmsMetadata, toExclude);

            assert vmsMetadata != null;

            // could be higher. must adjust according to the number of cores available
            ExecutorService readTaskPool = Executors.newSingleThreadExecutor();

            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

            // for now only giving support to one vms
            Optional<Map.Entry<String, Object>> entryOptional = vmsMetadata.loadedVmsInstances().entrySet().stream().findFirst();
            if(entryOptional.isEmpty()) throw new IllegalStateException("Cannot find a single instance of VMS");
            String vmsName = entryOptional.get().getKey();

            VmsTransactionScheduler scheduler =
                    new VmsTransactionScheduler(
                            readTaskPool,
                            vmsInternalPubSubService,
                            vmsMetadata.queueToVmsTransactionMap(), null);

            // ideally lastTid and lastBatch must be read from the storage

            VmsNode vmsIdentifier = new VmsNode(
                    host, port, vmsName,
                    0, 0,0,
                    vmsMetadata.dataSchema(),
                    vmsMetadata.inputEventSchema(),
                    vmsMetadata.outputEventSchema());

            // at least two, one for acceptor and one for new events
            ExecutorService socketPool = Executors.newFixedThreadPool(2);

            EmbeddedVmsEventHandler eventHandler = EmbeddedVmsEventHandler.buildWithDefaults(
                    vmsIdentifier, null,
                    transactionFacade, vmsInternalPubSubService, vmsMetadata, serdes, socketPool );

            /*
             one way to accomplish that, but that would require keep checking the thread status
            Thread eventHandlerThread = new Thread(eventHandler);
            eventHandlerThread.start();
            Thread schedulerThread = new Thread(scheduler);
            schedulerThread.start();
            */

            // this is not the cause since the threads continue running in background...
            CompletableFuture<?>[] futures = new CompletableFuture[2];
            futures[0] = CompletableFuture.runAsync( eventHandler );
            futures[1] = CompletableFuture.runAsync( scheduler );

            CompletableFuture.allOf(futures).join();

        } catch (Exception e) {
            logger.warning("Error on starting the VMS application: "+e.getMessage());
        }

        // abnormal termination
        System.exit(1);

    }

}
