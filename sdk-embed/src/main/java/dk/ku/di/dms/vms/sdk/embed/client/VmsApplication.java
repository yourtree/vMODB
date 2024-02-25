package dk.ku.di.dms.vms.sdk.embed.client;

import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataModel;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.transaction.TransactionManager;
import dk.ku.di.dms.vms.sdk.core.channel.IVmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadataLoader;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionScheduler;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbeddedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.handler.VmsEventHandler;
import dk.ku.di.dms.vms.sdk.embed.metadata.EmbedMetadataLoader;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;
import org.reflections.Reflections;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 * Starting point for initializing the VMS runtime
 */
public final class VmsApplication {

    private static final Logger logger = Logger.getLogger("VmsApplication");

    private final VmsRuntimeMetadata vmsRuntimeMetadata;

    private final Map<String, Table> catalog;

    private final VmsEventHandler eventHandler;

    private final StoppableRunnable scheduler;

    private final IVmsInternalChannels internalChannels;

    private VmsApplication(VmsRuntimeMetadata vmsRuntimeMetadata,
                           Map<String, Table> catalog,
                           VmsEventHandler eventHandler,
                           StoppableRunnable scheduler,
                           IVmsInternalChannels internalChannels) {
        this.vmsRuntimeMetadata = vmsRuntimeMetadata;
        this.catalog = catalog;
        this.eventHandler = eventHandler;
        this.scheduler = scheduler;
        this.internalChannels = internalChannels;
    }

    /**
     * This method initializes two threads:
     * (i) EventHandler, responsible for communicating with the external world
     * (ii) Scheduler, responsible for scheduling transactions and returning resulting events
     */
    public static VmsApplication build(String host, int port, String[] packages) throws Exception {

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

        Reflections reflections = VmsMetadataLoader.configureReflections( packages );

        Set<Class<?>> vmsClasses = reflections.getTypesAnnotatedWith(Microservice.class);
        if(vmsClasses.isEmpty()) throw new IllegalStateException("No classes annotated with @Microservice in this application.");
        Map<Class<?>, String> entityToTableNameMap = VmsMetadataLoader.loadVmsTableNames(reflections);
        Map<Class<?>, String> entityToVirtualMicroservice = VmsMetadataLoader.mapEntitiesToVirtualMicroservice(vmsClasses, entityToTableNameMap);
        Map<String, VmsDataModel> vmsDataModelMap = VmsMetadataLoader.buildVmsDataModel( entityToVirtualMicroservice, entityToTableNameMap );

        // load catalog so we can pass the table instance to proxy repository
        Map<String, Table> catalog = EmbedMetadataLoader.loadCatalog(vmsDataModelMap, entityToTableNameMap);

        // operational API and checkpoint API
        TransactionManager transactionManager = new TransactionManager(catalog);

        Map<String, Object> tableToRepositoryMap = EmbedMetadataLoader.loadRepositoryClasses( vmsClasses, entityToTableNameMap, catalog,  transactionManager );
        Map<String, List<Object>> vmsToRepositoriesMap = EmbedMetadataLoader.mapRepositoriesToVms(vmsClasses, entityToTableNameMap, tableToRepositoryMap);

        VmsRuntimeMetadata vmsMetadata = VmsMetadataLoader.load(
                reflections,
                vmsClasses,
                vmsDataModelMap,
                vmsToRepositoriesMap,
                tableToRepositoryMap
        );

        // for now only giving support to one vms
        Optional<Map.Entry<String, String>> entryOptional = vmsMetadata.clazzNameToVmsName().entrySet().stream().findFirst();
        if(entryOptional.isEmpty()) throw new IllegalStateException("Cannot find a single instance of VMS");
        String vmsName = entryOptional.get().getValue();

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        // ideally lastTid and lastBatch must be read from the storage
        VmsNode vmsIdentifier = new VmsNode(
                host, port, vmsName,
                0, 0,0,
                vmsMetadata.dataModel(),
                vmsMetadata.inputEventSchema(),
                vmsMetadata.outputEventSchema());

        // at least two, one for acceptor and one for new events
        ExecutorService socketPool = Executors.newFixedThreadPool(2);

        VmsEventHandler eventHandler = VmsEventHandler.build(
                vmsIdentifier, transactionManager, vmsInternalPubSubService, vmsMetadata, serdes, socketPool );

//        VmsComplexTransactionScheduler scheduler =
//                VmsComplexTransactionScheduler.build(
//                        vmsName,
//                        vmsInternalPubSubService,
//                        vmsMetadata.queueToVmsTransactionMap(),
//                        eventHandler.schedulerHandler());
        StoppableRunnable scheduler = VmsTransactionScheduler.build(
                vmsName,
                vmsInternalPubSubService,
                vmsMetadata.queueToVmsTransactionMap(),
                eventHandler.transactionalHandler(),
                eventHandler.schedulerHandler() );

        return new VmsApplication( vmsMetadata, catalog, eventHandler, scheduler, vmsInternalPubSubService );
    }

    public void start(){
        // one way to accomplish that, but that would require keep checking the thread status
        Thread eventHandlerThread = new Thread(this.eventHandler);
        eventHandlerThread.start();
        Thread schedulerThread = new Thread(this.scheduler);
        schedulerThread.start();
    }

    public void stop(){
        this.eventHandler.stop();
        this.scheduler.stop();
    }

    public IVmsInternalChannels internalChannels() {
        return this.internalChannels;
    }

    public Table getTable(String table){
        return this.catalog.get(table);
    }

    public Object getRepositoryProxy(String table){
        return this.vmsRuntimeMetadata.repositoryProxyMap().get(table);
    }

    public long lastTidFinished() {
        return this.eventHandler.lastTidFinished();
    }

    public Object getService() {
        return this.vmsRuntimeMetadata.loadedVmsInstances().entrySet().stream().findFirst().get();
    }

}
