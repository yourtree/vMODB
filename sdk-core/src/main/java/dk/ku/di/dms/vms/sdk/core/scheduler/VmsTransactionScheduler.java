package dk.ku.di.dms.vms.sdk.core.scheduler;

import dk.ku.di.dms.vms.modb.api.enums.ExecutionModeEnum;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionalHandler;
import dk.ku.di.dms.vms.sdk.core.channel.IVmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsTransactionMetadata;
import dk.ku.di.dms.vms.sdk.core.operational.ISchedulerCallback;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTask;
import dk.ku.di.dms.vms.sdk.core.scheduler.handlers.ICheckpointEventHandler;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * A transaction scheduler aware of the new concepts of partitioned and parallel.
 * Besides, for simplicity, it only consider transactions that spawn a single task
 * in each VMS (in opposite of possible many tasks as found in {@link VmsComplexTransactionScheduler}).
 */
public final class VmsTransactionScheduler extends StoppableRunnable {

    private final Logger logger = Logger.getLogger(VmsTransactionScheduler.class.getSimpleName());

    private final Map<Long, VmsTransactionTask> transactionTaskMap;

    // map the last tid
    private final Map<Long, Long> lastTidToTidMap;

    /**
     * Thread pool for partitioned and parallel tasks
     */
    private final ExecutorService sharedTaskPool;

    /**
     * Thread pool for single-threaded tasks
     */
    private final ExecutorService singleThreadPool;

    private final AtomicInteger numParallelTasksRunning = new AtomicInteger(0);

    private final AtomicInteger numPartitionedTasksRunning = new AtomicInteger(0);

    private final AtomicBoolean singleThreadTaskRunning = new AtomicBoolean(false);

    // the callback atomically updates this variable
    // used to track progress in the presence of parallel and partitioned tasks
    private volatile long lastFinishedTid = 0;

    private final Map<Long, OutboundEventResult> tasksPendingSubmission = new ConcurrentHashMap<>();

    private final Set<Object> partitionKeyTrackingMap = new HashSet<>();

    private final IVmsInternalChannels vmsChannels;

    // transaction metadata mapping
    private final Map<String, VmsTransactionMetadata> transactionMetadataMap;

    private final Collection<InboundEvent> localInputEvents;

    // used to receive external signals that require the scheduler to pause and run tasks, e.g., checkpointing
    private final ICheckpointEventHandler checkpointHandler;

    private final ITransactionalHandler transactionalHandler;

    // used to identify in which VMS this scheduler is running
    private final String vmsIdentifier;

    public static VmsTransactionScheduler build(String vmsIdentifier,
                                                IVmsInternalChannels vmsChannels,
                                                Map<String, VmsTransactionMetadata> transactionMetadataMap,
                                                ITransactionalHandler transactionalHandler,
                                                ICheckpointEventHandler checkpointHandler){
        // could be higher. must adjust according to the number of cores available
        return new VmsTransactionScheduler(
                vmsIdentifier,
                Executors.newWorkStealingPool(Runtime.getRuntime().availableProcessors() - 2),
                Executors.newSingleThreadExecutor(),
                vmsChannels,
                transactionMetadataMap,
                transactionalHandler,
                checkpointHandler);
    }

    private VmsTransactionScheduler(String vmsIdentifier,
                                    ExecutorService sharedTaskPool,
                                    ExecutorService singleThreadPool,
                                    IVmsInternalChannels vmsChannels,
                                    Map<String, VmsTransactionMetadata> transactionMetadataMap,
                                    ITransactionalHandler transactionalHandler,
                                    ICheckpointEventHandler checkpointHandler){
        super();

        this.vmsIdentifier = vmsIdentifier;
        // thread pools
        this.sharedTaskPool = sharedTaskPool;
        this.singleThreadPool = singleThreadPool;

        // infra (come from external)
        this.transactionMetadataMap = transactionMetadataMap;
        this.vmsChannels = vmsChannels;

        // operational (internal control of transactions and tasks)
        this.transactionTaskMap = new HashMap<>();
        this.transactionTaskMap.put( 0L, new VmsTransactionTask(0) );
        this.lastTidToTidMap = new HashMap<>();

        this.localInputEvents = new ArrayList<>(50);

        this.transactionalHandler = transactionalHandler;
        this.checkpointHandler = checkpointHandler;
    }

    /**
     * Inspired by <a href="https://stackoverflow.com/questions/826212/java-executors-how-to-be-notified-without-blocking-when-a-task-completes">link</a>,
     * this method can block on checkForNewEvents, leaving the task threads itself, via callback, modify
     * the class state appropriately. Care must be taken with some variables.
     */
    @Override
    public void run() {

        this.logger.info(this.vmsIdentifier+": Transaction scheduler has started");

        while(isRunning()) {
            try{
                this.checkForNewEvents();
                this.executeReadyTasks();
                if( this.checkpointHandler.mustCheckpoint() ) {
                    this.checkpointHandler.checkpoint();
                }
            } catch(Exception e){
                this.logger.warning(this.vmsIdentifier+": Error on scheduler loop: "+e.getMessage());
            }
        }
    }

    private final SchedulerCallback callback = new SchedulerCallback();

    private class SchedulerCallback implements ISchedulerCallback {

        @Override
        public void success(ExecutionModeEnum executionMode, OutboundEventResult outboundEventResult) {

            updateLastFinishedTid(outboundEventResult.tid());

            // my previous has sent the event already?
            VmsTransactionTask task = transactionTaskMap.get(outboundEventResult.tid());
            var lastTid = task.lastTid();
            if(transactionTaskMap.get(lastTid).status().value == 5){
                // logger.info("adding "+outboundEventResult.tid()+" to output queue...");

                vmsChannels.transactionOutputQueue().add(
                        new VmsTransactionResult(outboundEventResult.tid(),
                                List.of(outboundEventResult)) );

                task.signalOutputSent();

                // do I precede a pending submission?
                Long nextTid = lastTidToTidMap.get(outboundEventResult.tid());
                while(nextTid != null && tasksPendingSubmission.containsKey(nextTid)){
                    // send
                    var nextTask = tasksPendingSubmission.remove(nextTid);
                    if(nextTask == null) break;

                    // logger.info("adding "+nextTask.tid()+" to output queue...");
                    vmsChannels.transactionOutputQueue().add(
                            new VmsTransactionResult(nextTask.tid(),
                                    List.of(nextTask)) );

                    transactionTaskMap.get(nextTid).signalOutputSent();

                    nextTid = lastTidToTidMap.get(nextTid);
                }

            } else {
                tasksPendingSubmission.put( outboundEventResult.tid(), outboundEventResult );
            }

            if(executionMode == ExecutionModeEnum.SINGLE_THREADED)
                singleThreadTaskRunning.set(false);
            else if (executionMode == ExecutionModeEnum.PARALLEL) {
                numParallelTasksRunning.decrementAndGet();
            } else {
                if(task.partitionId().isPresent()){
                    partitionKeyTrackingMap.remove(task.partitionId().get());
                    numPartitionedTasksRunning.decrementAndGet();
                } else {
                    singleThreadTaskRunning.set(false);
                }
            }
        }

        @Override
        public void error(ExecutionModeEnum executionMode, long tid, Exception e) {
            // TODO handle errors
            logger.warning("Error captured in application execution: "+e.getMessage());
            if(executionMode == ExecutionModeEnum.SINGLE_THREADED)
                singleThreadTaskRunning.set(false);
            else if (executionMode == ExecutionModeEnum.PARALLEL) {
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
    }

    /**
     * This method makes sure that TIDs always increase
     * so the next single thread tasks can be executed
     */
    private synchronized void updateLastFinishedTid(long tid){
        if(tid > this.lastFinishedTid){
            this.lastFinishedTid = tid;
        }
    }

    /**
     * To avoid the scheduler to remain in a busy loop
     * while no new input events arrive
     */
    private boolean block = false;

    private void executeReadyTasks() {

        Long nextTid = this.lastTidToTidMap.get( this.lastFinishedTid );
        // if nextTid == null then the scheduler must block until a new event arrive to progress
        if(nextTid == null) {
            block = true;
            return;
        }

        VmsTransactionTask task = this.transactionTaskMap.get( nextTid );

        while(true) {

            // if (task == null) return;
            if(task.status().value > 1){
                return;
            }

            boolean singleThreadTask = false;

            switch (task.signature().executionMode()) {
                case SINGLE_THREADED -> {
                    if (this.numParallelTasksRunning.get() == 0 && numPartitionedTasksRunning.get() == 0) {
                        singleThreadTaskRunning.set(true);
                        task.signalReady();
                        this.singleThreadPool.submit(task);
                        singleThreadTask = true;
                    } else {
                        return;
                    }
                }
                case PARALLEL -> {
                    if (!singleThreadTaskRunning.get() && numPartitionedTasksRunning.get() == 0) {
                        this.numParallelTasksRunning.incrementAndGet();
                        task.signalReady();
                        this.sharedTaskPool.submit(task);
                    } else {
                        return;
                    }
                }
                case PARTITIONED -> {
                    if (singleThreadTaskRunning.get() || numParallelTasksRunning.get() > 0) {
                        return;
                    }

                    if(task.partitionId().isEmpty()){
                        // this.logger.warning(this.vmsIdentifier + ": Task "+task.tid()+" will run as single-threaded even though it is marked as partitioned");
                        if (this.numParallelTasksRunning.get() == 0 && numPartitionedTasksRunning.get() == 0) {
                            singleThreadTaskRunning.set(true);
                            task.signalReady();
                            this.singleThreadPool.submit(task);
                            singleThreadTask = true;
                        } else {
                            return;
                        }
                    }

                    if (!this.partitionKeyTrackingMap.contains(task.partitionId().get())) {
                        this.partitionKeyTrackingMap.add(task.partitionId().get());
                        this.numPartitionedTasksRunning.incrementAndGet();
                        task.signalReady();
                        this.sharedTaskPool.submit(task);
                    } else {
                        return;
                    }

                }
            }

            // bypass the single-thread execution if possible
            if(!singleThreadTask && this.lastTidToTidMap.get( task.tid() ) != null ){
                task = this.transactionTaskMap.get( this.lastTidToTidMap.get( task.tid() ) );
            }

        }

    }

    private void checkForNewEvents() throws InterruptedException {

        if(this.vmsChannels.transactionInputQueue().isEmpty()){
            if(block) {
                this.localInputEvents.add(this.vmsChannels.transactionInputQueue().take());
                block = false;
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
            logger.warning(this.vmsIdentifier+": Event TID has already been processed! Queue '" + inboundEvent.event() + "' Batch: " + inboundEvent.batch() + " TID: " + inboundEvent.tid());
            return;
        }

        this.transactionTaskMap.put(inboundEvent.tid(), new VmsTransactionTask(
                this.transactionalHandler,
                this.callback,
                inboundEvent.tid(),
                inboundEvent.lastTid(),
                inboundEvent.batch(),
                this.transactionMetadataMap
                        .get(inboundEvent.event())
                        .signatures.get(0).object(),
                inboundEvent.input()
        ));

        // mark the last tid, so we can get the next to execute when appropriate
        this.lastTidToTidMap.put( inboundEvent.lastTid(), inboundEvent.tid() );
    }

}
