package dk.ku.di.dms.vms.sdk.core.scheduler;

import dk.ku.di.dms.vms.modb.api.enums.ExecutionModeEnum;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionalHandler;
import dk.ku.di.dms.vms.sdk.core.channel.IVmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsTransactionMetadata;
import dk.ku.di.dms.vms.sdk.core.operational.ISchedulerCallback;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTaskBuilder;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTaskBuilder.VmsTransactionTask;
import dk.ku.di.dms.vms.sdk.core.scheduler.complex.VmsComplexTransactionScheduler;
import dk.ku.di.dms.vms.sdk.core.scheduler.handlers.ICheckpointEventHandler;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.System.Logger.Level.*;

/**
 * A transaction scheduler aware of partitioned and parallel tasks.
 * Besides, for simplicity, it only considers transactions (i.e., event inputs) that spawn a single task
 * in each VMS (in opposite of possible many tasks as found in {@link VmsComplexTransactionScheduler}).
 */
public final class VmsTransactionScheduler extends StoppableRunnable {

    private static final System.Logger LOGGER = System.getLogger(VmsTransactionScheduler.class.getName());

    // must be concurrent since different threads are writing and reading from it concurrently
    private final Map<Long, VmsTransactionTask> transactionTaskMap;

    // map the last tid
    private final Map<Long, Long> lastTidToTidMap;

    /**
     * Thread pool for partitioned and parallel tasks
     */
    private final ExecutorService sharedTaskPool;

    private final AtomicInteger numParallelTasksRunning = new AtomicInteger(0);

    private final AtomicInteger numPartitionedTasksRunning = new AtomicInteger(0);

    private final AtomicBoolean singleThreadTaskRunning = new AtomicBoolean(false);

    // the callback atomically updates this variable
    // used to track progress in the presence of parallel and partitioned tasks
    private volatile long lastTidFinished;

    private final Set<Object> partitionKeyTrackingMap = new HashSet<>();

    private final IVmsInternalChannels vmsChannels;

    // transaction metadata mapping
    private final Map<String, VmsTransactionMetadata> transactionMetadataMap;

    private final Collection<InboundEvent> localInputEvents;

    // used to receive external signals that require the scheduler to pause and run tasks, e.g., checkpointing
    private final ICheckpointEventHandler checkpointHandler;

    // used to identify in which VMS this scheduler is running
    private final String vmsIdentifier;

    private final VmsTransactionTaskBuilder vmsTransactionTaskBuilder;

    public static VmsTransactionScheduler build(String vmsIdentifier,
                                                IVmsInternalChannels vmsChannels,
                                                Map<String, VmsTransactionMetadata> transactionMetadataMap,
                                                ITransactionalHandler transactionalHandler,
                                                ICheckpointEventHandler checkpointHandler,
                                                int vmsThreadPoolSize){
        return new VmsTransactionScheduler(
                vmsIdentifier,
                Executors.newWorkStealingPool(vmsThreadPoolSize),
                vmsChannels,
                transactionMetadataMap,
                transactionalHandler,
                checkpointHandler);
    }

    private VmsTransactionScheduler(String vmsIdentifier,
                                    ExecutorService sharedTaskPool,
                                    IVmsInternalChannels vmsChannels,
                                    Map<String, VmsTransactionMetadata> transactionMetadataMap,
                                    ITransactionalHandler transactionalHandler,
                                    ICheckpointEventHandler checkpointHandler){
        super();

        this.vmsIdentifier = vmsIdentifier;
        // thread pools
        this.sharedTaskPool = sharedTaskPool;

        // infra (come from external)
        this.transactionMetadataMap = transactionMetadataMap;
        this.vmsChannels = vmsChannels;

        // operational (internal control of transactions and tasks)
        this.transactionTaskMap = new ConcurrentHashMap<>();
        SchedulerCallback callback = new SchedulerCallback();
        this.vmsTransactionTaskBuilder = new VmsTransactionTaskBuilder(transactionalHandler, callback);
        this.transactionTaskMap.put( 0L, vmsTransactionTaskBuilder.buildFinished(0) );
        this.lastTidToTidMap = new HashMap<>();

        this.localInputEvents = new ArrayList<>(50);

        this.checkpointHandler = checkpointHandler;
    }

    /**
     * Inspired by <a href="https://stackoverflow.com/questions/826212/java-executors-how-to-be-notified-without-blocking-when-a-task-completes">link</a>,
     * this method can block on checkForNewEvents, leaving the task threads itself, via callback, modify
     * the class state appropriately. Care must be taken with some variables.
     */
    @Override
    public void run() {

        LOGGER.log(INFO,this.vmsIdentifier+": Transaction scheduler has started");

        while(this.isRunning()) {
            try{
                this.checkForNewEvents();
                this.executeReadyTasks();
                if( this.checkpointHandler.mustCheckpoint() ) {
                    this.checkpointHandler.checkpoint();
                }
            } catch(Exception e){
                LOGGER.log(WARNING, this.vmsIdentifier+": Error on scheduler loop: "+e.getCause().getMessage());
            }
        }
    }

    private final class SchedulerCallback implements ISchedulerCallback, Thread.UncaughtExceptionHandler {

        @Override
        public void success(ExecutionModeEnum executionMode, OutboundEventResult outboundEventResult) {

            updateLastFinishedTid(outboundEventResult.tid());

            // my previous has sent the event already?
            VmsTransactionTask task = transactionTaskMap.get(outboundEventResult.tid());
            var resultToQueue = new VmsTransactionResult(
                    outboundEventResult.tid(),
                    outboundEventResult);
            boolean sent = vmsChannels.transactionOutputQueue().offer(resultToQueue);
            while(!sent) {
                sent = vmsChannels.transactionOutputQueue().offer(resultToQueue);
            }

            if(executionMode == ExecutionModeEnum.SINGLE_THREADED)
                singleThreadTaskRunning.set(false);
            else if (executionMode == ExecutionModeEnum.PARALLEL) {
                numParallelTasksRunning.decrementAndGet();
            } else {
                if(task.partitionId().isPresent()){
                    partitionKeyTrackingMap.remove(task.partitionId().get());
                    numPartitionedTasksRunning.decrementAndGet();
                    LOGGER.log(DEBUG, vmsIdentifier+": Partitioned task "+task.tid()+" finished execution.");
                } else {
                    singleThreadTaskRunning.set(false);
                }
            }
        }

        @Override
        public void error(ExecutionModeEnum executionMode, long tid, Exception e) {
            // TODO handle errors
            LOGGER.log(WARNING, "Error captured during application execution: \n"+e.getCause().getMessage());
            if(executionMode == ExecutionModeEnum.SINGLE_THREADED) {
                singleThreadTaskRunning.set(false);
            } else if (executionMode == ExecutionModeEnum.PARALLEL) {
                numParallelTasksRunning.decrementAndGet();
            } else {
                VmsTransactionTask task = transactionTaskMap.get(tid);
                if(task.partitionId().isPresent()){
                    partitionKeyTrackingMap.remove(task.partitionId().get());
                    numPartitionedTasksRunning.decrementAndGet();
                } else {
                    singleThreadTaskRunning.set(false);
                }
            }
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            LOGGER.log(ERROR, "Uncaught exception captured during application execution: \n"+e.getCause().getMessage());
        }

    }

    /**
     * This method makes sure that TIDs always increase
     * so the next single thread tasks can be executed
     */
    private void updateLastFinishedTid(long tid){
        if (tid < this.lastTidFinished) return;
        synchronized (this) {
            if (tid > this.lastTidFinished) {
                this.lastTidFinished = tid;
            }
        }
    }

    /**
     * To avoid the scheduler to remain in a busy loop
     * while no new input events arrive
     */
    private boolean BLOCKING = false;

    private void executeReadyTasks() {

        Long nextTid = this.lastTidToTidMap.get(this.lastTidFinished);
        // if nextTid == null then the scheduler must block until a new event arrive to progress
        if(nextTid == null) {
            this.BLOCKING = true;
            return;
        }

        VmsTransactionTask task = this.transactionTaskMap.get( nextTid );

        while(true) {

            if(task.isScheduled()){
                return;
            }

            switch (task.signature().executionMode()) {
                case SINGLE_THREADED -> {
                    if (this.canSingleThreadTaskRun()) {
                        LOGGER.log(DEBUG, this.vmsIdentifier+": Scheduling single-thread task "+task.tid()+" for execution...");
                        this.submitSingleThreadTaskForExecution(task);
                    } else {
                        return;
                    }
                }
                case PARALLEL -> {
                    if (!this.singleThreadTaskRunning.get() && this.numPartitionedTasksRunning.get() == 0) {
                        this.numParallelTasksRunning.incrementAndGet();
                        task.signalReady();
                        LOGGER.log(DEBUG, this.vmsIdentifier+": Scheduling parallel task "+task.tid()+" for execution...");
                        this.sharedTaskPool.submit(task);
                    } else {
                        return;
                    }
                }
                case PARTITIONED -> {
                    if (this.singleThreadTaskRunning.get() || this.numParallelTasksRunning.get() > 0) {
                        return;
                    }

                    if(task.partitionId().isEmpty()){
                        // logger.warning(this.vmsIdentifier + ": Task "+task.tid()+" will run as single-threaded even though it is marked as partitioned");
                        if (this.canSingleThreadTaskRun()) {
                            this.submitSingleThreadTaskForExecution(task);
                        } else {
                            return;
                        }
                    }

                    if (!this.partitionKeyTrackingMap.contains(task.partitionId().get())) {
                        this.partitionKeyTrackingMap.add(task.partitionId().get());
                        this.numPartitionedTasksRunning.incrementAndGet();
                        task.signalReady();
                        LOGGER.log(DEBUG, this.vmsIdentifier+": Scheduling partitioned task "+task.tid()+" for execution...");
                        this.sharedTaskPool.submit(task);
                    } else {
                        return;
                    }

                }
            }

            // bypass the single-thread execution if possible
            if(!this.singleThreadTaskRunning.get() && this.lastTidToTidMap.get( task.tid() ) != null ){
                task = this.transactionTaskMap.get( this.lastTidToTidMap.get( task.tid() ) );
            }

        }

    }

    private void submitSingleThreadTaskForExecution(VmsTransactionTask task) {
        this.singleThreadTaskRunning.set(true);
        task.signalReady();
        this.sharedTaskPool.submit(task);
    }

    private boolean canSingleThreadTaskRun() {
        return this.numParallelTasksRunning.get() == 0 && numPartitionedTasksRunning.get() == 0;
    }

    private static final int MAX_SLEEP = 1000;

    private void checkForNewEvents() throws InterruptedException {

        if(this.vmsChannels.transactionInputQueue().isEmpty()){
            if(this.BLOCKING) {
                InboundEvent e = null;
                int pollTimeout = 1;
                while(e == null) {
                    pollTimeout = Math.min(pollTimeout * 2, MAX_SLEEP);
                    LOGGER.log(DEBUG,this.vmsIdentifier+": Transaction scheduler going to sleep for "+pollTimeout+" until new event arrives");
                    e = this.vmsChannels.transactionInputQueue().poll(pollTimeout, TimeUnit.MILLISECONDS);
                }
                this.localInputEvents.add(e);
                // disable block
                this.BLOCKING = false;
            } else {
                return;
            }
        }

        this.vmsChannels.transactionInputQueue().drainTo(this.localInputEvents);

        for (InboundEvent input : this.localInputEvents) {
            this.processNewEvent(input);
        }

        // clear previous round
        this.localInputEvents.clear();

    }

    private void processNewEvent(InboundEvent inboundEvent) {
        if (this.transactionTaskMap.containsKey(inboundEvent.tid())) {
            LOGGER.log(WARNING, this.vmsIdentifier+": Event TID has already been processed! Queue '" + inboundEvent.event() + "' Batch: " + inboundEvent.batch() + " TID: " + inboundEvent.tid());
            return;
        }

        this.transactionTaskMap.put(inboundEvent.tid(), this.vmsTransactionTaskBuilder.build(
                inboundEvent.tid(),
                inboundEvent.lastTid(),
                inboundEvent.batch(),
                this.transactionMetadataMap
                        .get(inboundEvent.event())
                        .signatures.getFirst().object(),
                inboundEvent.input()
        ));

        // mark the last tid, so we can get the next to execute when appropriate
        this.lastTidToTidMap.put( inboundEvent.lastTid(), inboundEvent.tid() );
    }

    public long lastTidFinished(){
        return this.lastTidFinished;
    }

}
