package dk.ku.di.dms.vms.sdk.embed.client;

import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataModel;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
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

/**
 * Starting point for initializing the VMS application runtime
 */
public final class VmsApplication {

    private final String name;

    private final VmsRuntimeMetadata vmsRuntimeMetadata;

    private final Map<String, Table> catalog;

    private final VmsEventHandler eventHandler;

    private final ITransactionManager transactionManager;

    private final StoppableRunnable transactionScheduler;

    private final IVmsInternalChannels internalChannels;

    private VmsApplication(String name,
                           VmsRuntimeMetadata vmsRuntimeMetadata,
                           Map<String, Table> catalog,
                           VmsEventHandler eventHandler,
                           ITransactionManager transactionManager,
                           StoppableRunnable transactionScheduler,
                           IVmsInternalChannels internalChannels) {
        this.name = name;
        this.vmsRuntimeMetadata = vmsRuntimeMetadata;
        this.catalog = catalog;
        this.eventHandler = eventHandler;
        this.transactionManager = transactionManager;
        this.transactionScheduler = transactionScheduler;
        this.internalChannels = internalChannels;
    }

    /**
     * This method initializes two threads:
     * (i) EventHandler, responsible for communicating with the external world
     * (ii) Scheduler, responsible for scheduling transactions and returning resulting events
     */
    public static VmsApplication build(VmsApplicationOptions options) throws Exception {

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

        Reflections reflections = VmsMetadataLoader.configureReflections(options.packages() );

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
                options.host(), options.port(), vmsName,
                0, 0,0,
                vmsMetadata.dataModel(),
                vmsMetadata.inputEventSchema(),
                vmsMetadata.outputEventSchema());

        VmsEventHandler eventHandler = VmsEventHandler.build(
                vmsIdentifier, transactionManager, vmsInternalPubSubService, vmsMetadata, options, serdes);

//        VmsComplexTransactionScheduler scheduler =
//                VmsComplexTransactionScheduler.build(
//                        vmsName,
//                        vmsInternalPubSubService,
//                        vmsMetadata.queueToVmsTransactionMap(),
//                        eventHandler.schedulerHandler());

        // could be higher. must adjust according to the number of cores available
        // why minus 2? to account for the event handler and the scheduler
        int threadPoolSize = options.vmsThreadPoolSize() > 0 ? options.vmsThreadPoolSize() : Runtime.getRuntime().availableProcessors();
        StoppableRunnable transactionScheduler = VmsTransactionScheduler.build(
                vmsName,
                vmsInternalPubSubService,
                vmsMetadata.queueToVmsTransactionMap(),
                eventHandler.transactionalHandler(),
                eventHandler.schedulerHandler(),
                threadPoolSize );

        return new VmsApplication( vmsName, vmsMetadata, catalog, eventHandler, transactionManager, transactionScheduler, vmsInternalPubSubService );
    }

    /**
     * Setting high priority can increase the garbage size, leading to larger garbage collection times
     */
    public void start(){
        Thread eventHandlerThread = new Thread(this.eventHandler);
        eventHandlerThread.setName("vms-event-handler-"+this.name);
        eventHandlerThread.start();
        Thread transactionSchedulerThread = new Thread(this.transactionScheduler);
        eventHandlerThread.setName("vms-transaction-scheduler-"+this.name);
        transactionSchedulerThread.start();
    }

    public void stop(){
        this.eventHandler.stop();
        this.transactionScheduler.stop();
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
        return ((VmsTransactionScheduler)this.transactionScheduler).lastTidFinished();
    }

    public Object getService() {
        var service = this.vmsRuntimeMetadata.loadedVmsInstances().entrySet().iterator().next().getValue();
        if(service != null) {
            return service;
        }
        throw new RuntimeException("Service not loaded");
    }

    public ITransactionManager getTransactionManager() {
        return this.transactionManager;
    }

}
