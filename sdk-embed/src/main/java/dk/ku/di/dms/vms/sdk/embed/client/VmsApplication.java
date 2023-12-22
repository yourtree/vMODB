package dk.ku.di.dms.vms.sdk.embed.client;

import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.transaction.TransactionFacade;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionScheduler;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbeddedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.handler.EmbeddedVmsEventHandler;
import dk.ku.di.dms.vms.sdk.embed.metadata.EmbedMetadataLoader;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Starting point for initializing the VMS runtime
 */
public final class VmsApplication {

    private static final Logger logger = Logger.getLogger("VmsApplication");

    private final VmsRuntimeMetadata vmsRuntimeMetadata;

    private final Map<String, Table> catalog;

    private final EmbeddedVmsEventHandler eventHandler;

    private final VmsTransactionScheduler scheduler;

    private VmsApplication(VmsRuntimeMetadata vmsRuntimeMetadata, Map<String, Table> catalog, EmbeddedVmsEventHandler eventHandler, VmsTransactionScheduler scheduler) {
        this.vmsRuntimeMetadata = vmsRuntimeMetadata;
        this.catalog = catalog;
        this.eventHandler = eventHandler;
        this.scheduler = scheduler;
    }

    public Table getTable(String table){
        return this.catalog.get(table);
    }

    /**
     * This method initializes two threads:
     * (i) EventHandler, responsible for communicating with the external world
     * (ii) Scheduler, responsible for scheduling transactions and returning resulting events
     */
    public static VmsApplication build(String host, int port, String[] packages, String... entitiesToExclude) throws Exception {

        // check first whether we are in decoupled or embed mode
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

        if(packageName.equalsIgnoreCase("Nothing")) throw new IllegalStateException("Cannot identify package.");

        VmsEmbeddedInternalChannels vmsInternalPubSubService = new VmsEmbeddedInternalChannels();

        VmsRuntimeMetadata vmsMetadata = EmbedMetadataLoader.loadRuntimeMetadata(packages);

        if( vmsMetadata == null ) throw new IllegalStateException("Cannot build VMS runtime metadata.");

        // for now only giving support to one vms
        Optional<Map.Entry<String, Object>> entryOptional = vmsMetadata.loadedVmsInstances().entrySet().stream().findFirst();
        if(entryOptional.isEmpty()) throw new IllegalStateException("Cannot find a single instance of VMS");
        String vmsName = entryOptional.get().getKey();

        Set<String> toExclude = entitiesToExclude != null ? Arrays.stream(entitiesToExclude).collect(
                Collectors.toSet()) : new HashSet<>();

        // load catalog
        Map<String, Table> catalog = EmbedMetadataLoader.loadCatalog(vmsMetadata, toExclude);

        TransactionFacade transactionFacade = EmbedMetadataLoader.loadTransactionFacadeAndInjectIntoRepositories(vmsMetadata, catalog);

        // could be higher. must adjust according to the number of cores available
        ExecutorService readTaskPool = Executors.newSingleThreadExecutor();

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

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

        return new VmsApplication( vmsMetadata, catalog, eventHandler, scheduler );

    }

    public void start(){
        // one way to accomplish that, but that would require keep checking the thread status
        Thread eventHandlerThread = new Thread(eventHandler);
        eventHandlerThread.start();
        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.start();
    }

    public void stop(){
        eventHandler.stop();
        scheduler.stop();
    }

}
