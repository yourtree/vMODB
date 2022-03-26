package dk.ku.di.dms.vms.sdk.core.manager;

import dk.ku.di.dms.vms.sdk.core.event.EventChannel;
import dk.ku.di.dms.vms.sdk.core.event.IEventHandler;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadata;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadataLoader;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionExecutor;
import dk.ku.di.dms.vms.sdk.core.scheduler.Scheduler;

import java.util.concurrent.*;

/**
 * Manager is a class that manages the lifecycle of components:
 * {@link Scheduler}, {@link IEventHandler}, and {@link VmsTransactionExecutor}
 */
public final class Manager implements Runnable {

    private final ManagerMetadata metadata;

    public Manager(ManagerMetadata metadata) throws Exception {
        if( metadata == null ) {
            this.metadata = new ManagerMetadata();
        } else {
            this.metadata = metadata;
        }

        if ( !this.metadata.initialized ){
            int availableCPUs = Runtime.getRuntime().availableProcessors();
            final ExecutorService executorService;
            if(availableCPUs == 1){
                executorService = ForkJoinPool.commonPool();
            } else {
                // 1 CPU is free for OS and JVM tasks
                executorService = Executors.newFixedThreadPool(availableCPUs - 1);
            }

            this.metadata.executorService = executorService;

            this.metadata.eventChannel = new EventChannel();

            // application loader
            VmsMetadataLoader loader = new VmsMetadataLoader();

            VmsMetadata config = loader.load(null);

            // event handler
            // TPCCEventHandler eventHandler = new TPCCEventHandler(eventRepository);

            // scheduler
            this.metadata.scheduler = new Scheduler(this.metadata.eventChannel, config.eventToVmsTransactionMap());

            // executor
            this.metadata.executor = new VmsTransactionExecutor(this.metadata.eventChannel);

        }

    }

    private void doHealthCheck(){

        // TODO verify how healthy the threads are

        // the future interface is done is cancelled ...

    }


    @Override
    public void run() {

        // does a thread has stopped suddenly?
        // do we have several events without being processed?
        // is there some problem that should be reported to the log?

        // https://stackoverflow.com/questions/1323408/get-a-list-of-all-threads-currently-running-in-java/3018672#3018672
        // https://stackoverflow.com/questions/1323408/get-a-list-of-all-threads-currently-running-in-java

        if ( !this.metadata.initialized ) {

            // 1. start scheduler
            this.metadata.schedulerFuture = this.metadata.executorService.submit(this.metadata.scheduler);

            // 2. start event handler
            this.metadata.eventHandlerFuture = this.metadata.executorService.submit(this.metadata.eventHandler);

            // 3. start data operation executor
            this.metadata.executorFuture = this.metadata.executorService.submit(this.metadata.executor);

            this.metadata.initialized = true;

            return;
        }

    }


}
