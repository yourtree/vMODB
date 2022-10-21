package dk.ku.di.dms.vms.sdk.embed.client;

import dk.ku.di.dms.vms.modb.common.schema.network.VmsIdentifier;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.transaction.TransactionFacade;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.handler.EmbedVmsEventHandler;
import dk.ku.di.dms.vms.sdk.embed.metadata.EmbedMetadataLoader;
import dk.ku.di.dms.vms.sdk.embed.scheduler.EmbedVmsTransactionScheduler;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
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

            VmsEmbedInternalChannels vmsInternalPubSubService = new VmsEmbedInternalChannels();

            VmsRuntimeMetadata vmsMetadata = EmbedMetadataLoader.loadRuntimeMetadata(packages);

            Set<String> toExclude = entitiesToExclude != null ? Arrays.stream(entitiesToExclude).collect(
                    Collectors.toSet()) : new HashSet<>();

            TransactionFacade transactionFacade = EmbedMetadataLoader.loadTransactionFacade(vmsMetadata, toExclude);

            assert vmsMetadata != null;

            ExecutorService readTaskPool = Executors.newSingleThreadExecutor();

            IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

            // for now only giving support to one vms
            String vmsName = vmsMetadata.loadedVmsInstances().entrySet().stream().findFirst().get().getKey();

            EmbedVmsTransactionScheduler scheduler =
                    new EmbedVmsTransactionScheduler(
                            readTaskPool,
                            vmsInternalPubSubService,
                            vmsMetadata.queueToVmsTransactionMap(),
                            vmsMetadata.queueToEventMap(),
                            serdes,
                            transactionFacade);

            // ideally lastTid and lastBatch must be read from the log

            VmsIdentifier vmsIdentifier = new VmsIdentifier(
                    host, port, vmsName,
                    0, 0,
                    vmsMetadata.dataSchema(),
                    vmsMetadata.inputEventSchema(),
                    vmsMetadata.outputEventSchema());

            ExecutorService socketPool = Executors.newFixedThreadPool(2);

            EmbedVmsEventHandler eventHandler = new EmbedVmsEventHandler(
                    vmsInternalPubSubService, vmsIdentifier, vmsMetadata, serdes );

//            Thread eventHandlerThread = new Thread(eventHandler);
//            eventHandlerThread.start();
//            Thread schedulerThread = new Thread(scheduler);
//            schedulerThread.start();

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
