package dk.ku.di.dms.vms.sdk.core.scheduler;

import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.enums.ExecutionModeEnum;
import dk.ku.di.dms.vms.modb.common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.sdk.core.channel.IVmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsTransactionMetadata;
import dk.ku.di.dms.vms.sdk.core.operational.ISchedulerCallback;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTaskBuilder;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTaskBuilder.VmsTransactionTask;
import dk.ku.di.dms.vms.sdk.core.scheduler.complex.VmsComplexTransactionScheduler;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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

    private volatile boolean singleThreadTaskRunning = false;

    // the callback atomically updates this variable
    // used to track progress in the presence of parallel and partitioned tasks
    private final AtomicLong lastTidFinished;

    private final Set<Object> partitionKeyTrackingMap = ConcurrentHashMap.newKeySet();

    private final IVmsInternalChannels vmsChannels;

    // transaction metadata mapping
    private final Map<String, VmsTransactionMetadata> transactionMetadataMap;

    // used to identify in which VMS this scheduler is running
    private final String vmsIdentifier;

    private final VmsTransactionTaskBuilder vmsTransactionTaskBuilder;

    private final int maxSleep;

    private final int maxToDrain;

    public static VmsTransactionScheduler build(String vmsIdentifier,
                                                IVmsInternalChannels vmsChannels,
                                                Map<String, VmsTransactionMetadata> transactionMetadataMap,
                                                ITransactionManager transactionalHandler,
                                                int vmsThreadPoolSize,
                                                int maxSleep){
        LOGGER.log(INFO, vmsIdentifier+ ": Building transaction scheduler with thread pool size of "+ vmsThreadPoolSize);
        return new VmsTransactionScheduler(
                vmsIdentifier,
                vmsThreadPoolSize == 0 ? ForkJoinPool.commonPool() : Executors.newWorkStealingPool(vmsThreadPoolSize),
                vmsChannels,
                transactionMetadataMap,
                transactionalHandler,
                maxSleep);
    }

    private VmsTransactionScheduler(String vmsIdentifier,
                                    ExecutorService sharedTaskPool,
                                    IVmsInternalChannels vmsChannels,
                                    Map<String, VmsTransactionMetadata> transactionMetadataMap,
                                    ITransactionManager transactionalHandler, int maxSleep){
        super();

        this.vmsIdentifier = vmsIdentifier;
        // thread pools
        this.sharedTaskPool = sharedTaskPool;

        // infra (come from external)
        this.transactionMetadataMap = transactionMetadataMap;
        this.vmsChannels = vmsChannels;

        // operational (internal control of transactions and tasks)
        this.transactionTaskMap = new ConcurrentHashMap<>(1000000);
        SchedulerCallback callback = new SchedulerCallback();
        this.vmsTransactionTaskBuilder = new VmsTransactionTaskBuilder(transactionalHandler, callback);
        this.transactionTaskMap.put( 0L, this.vmsTransactionTaskBuilder.buildFinished(0) );
        this.lastTidToTidMap = new HashMap<>(1000000);

        this.lastTidFinished = new AtomicLong(0);
        this.maxSleep = maxSleep;
        this.maxToDrain = Runtime.getRuntime().availableProcessors() * 2;
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
            try {
                this.checkForNewEvents();
                this.executeReadyTasks();
            } catch(Exception e){
                e.printStackTrace(System.out);
                LOGGER.log(ERROR, this.vmsIdentifier+": Error on scheduler loop: "+(e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
            }
        }
        LOGGER.log(INFO,this.vmsIdentifier+": Transaction scheduler has terminated");
    }

    private final class SchedulerCallback implements ISchedulerCallback, Thread.UncaughtExceptionHandler {
        @Override
        public void success(ExecutionModeEnum executionMode, OutboundEventResult outboundEventResult) {
            VmsTransactionTask task = transactionTaskMap.get(outboundEventResult.tid());
            task.signalFinished();
            updateLastFinishedTid(outboundEventResult.tid());
            // my previous has sent the event already?
            vmsChannels.transactionOutputQueue().add(outboundEventResult);
            if(executionMode == ExecutionModeEnum.SINGLE_THREADED) {
                singleThreadTaskRunning = false;
            } else if (executionMode == ExecutionModeEnum.PARALLEL) {
                numParallelTasksRunning.decrementAndGet();
            } else {
                if(task.partitionId().isPresent()){
                    if(!partitionKeyTrackingMap.remove(task.partitionId().get())){
                        LOGGER.log(WARNING, vmsIdentifier+": Partitioned task "+task.tid()+" did not find its partition ID ("+task.partitionId().get()+") in the tracking map!");
                    }
                    numPartitionedTasksRunning.decrementAndGet();
                    LOGGER.log(DEBUG, vmsIdentifier + ": Partitioned task " + task.tid() + " finished execution.");
                } else {
                    singleThreadTaskRunning = false;
                }
            }
        }

        @Override
        public void error(ExecutionModeEnum executionMode, long tid, Exception e) {
            // a simple mechanism to handle error is by re-executing, depending on the nature of the error
            // if constraint violation, it cannot be re-executed
            // in this case, the error must be informed to the event handler, so the event handler
            // can forward the error to downstream VMSs. if input VMS, easier to handle, just send a noop to them
            LOGGER.log(WARNING, "Error captured during application execution: \n"+e.getCause().getMessage());
            if(executionMode == ExecutionModeEnum.SINGLE_THREADED) {
                singleThreadTaskRunning = false;
            } else if (executionMode == ExecutionModeEnum.PARALLEL) {
                numParallelTasksRunning.decrementAndGet();
            } else {
                VmsTransactionTask task = transactionTaskMap.get(tid);
                if(task.partitionId().isPresent()){
                    if(!partitionKeyTrackingMap.remove(task.partitionId().get())){
                        LOGGER.log(WARNING, vmsIdentifier+": Partitioned task "+task.tid()+" did not find its partition ID ("+task.partitionId().get()+") in the tracking map!");
                    }
                    numPartitionedTasksRunning.decrementAndGet();
                } else {
                    singleThreadTaskRunning = false;
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
    private void updateLastFinishedTid(final long tid){
        if(this.lastTidFinished.get() > tid) return;
        this.lastTidFinished.updateAndGet(currTid -> Math.max(currTid, tid));
    }

    /**
     * To avoid the scheduler to remain in a busy loop while no new input events arrive
     */
    private boolean mustWaitForInputEvent = false;

    private void executeReadyTasks() {
        Long nextTid = this.lastTidToTidMap.get(this.lastTidFinished.get());
        // if nextTid == null then the scheduler must block until a new event arrive to progress
        if(nextTid == null) {
            // keep scheduler sleeping since next tid is unknown
            this.mustWaitForInputEvent = true;
            return;
        }
        VmsTransactionTask task = this.transactionTaskMap.get( nextTid );
        while(true) {
            if(task.isScheduled()){
                return;
            }
            // must check because partitioned task interleave and may finish before a lower TID
            if(task.isFinished()){
                this.updateLastFinishedTid(nextTid);
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
                    if (!this.singleThreadTaskRunning && this.numPartitionedTasksRunning.get() == 0) {
                        this.numParallelTasksRunning.incrementAndGet();
                        task.signalReady();
                        LOGGER.log(DEBUG, this.vmsIdentifier+": Scheduling parallel task "+task.tid()+" for execution...");
                        this.sharedTaskPool.submit(task);
                    } else {
                        return;
                    }
                }
                case PARTITIONED -> {
                    if (this.singleThreadTaskRunning || this.numParallelTasksRunning.get() > 0) {
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
            if(!this.singleThreadTaskRunning && this.lastTidToTidMap.get( task.tid() ) != null ){
                task = this.transactionTaskMap.get( this.lastTidToTidMap.get( task.tid() ) );
            }
        }
    }

    private void submitSingleThreadTaskForExecution(VmsTransactionTask task) {
        this.singleThreadTaskRunning = true;
        task.signalReady();
        // can the scheduler itself run it? task.run();
        // this.sharedTaskPool.submit(task);
        task.run();
    }

    private boolean canSingleThreadTaskRun() {
        return this.numParallelTasksRunning.get() == 0 && numPartitionedTasksRunning.get() == 0;
    }

    List<InboundEvent> drained = new ArrayList<>(1024*10);

    private void checkForNewEvents() throws InterruptedException {
        InboundEvent inboundEvent;
        if(this.mustWaitForInputEvent) {
            inboundEvent = this.vmsChannels.transactionInputQueue().take();
//            int pollTimeout = Math.min(1, this.maxSleep);
//            while((inboundEvent = this.vmsChannels.transactionInputQueue().poll()) == null) {
//                LOGGER.log(DEBUG,this.vmsIdentifier+": Transaction scheduler going to sleep for "+pollTimeout+" until new event arrives");
//                this.giveUpCpu(pollTimeout);
//                pollTimeout = Math.min(pollTimeout + pollTimeout, this.maxSleep);
//            }
            // disable block
            this.mustWaitForInputEvent = false;
        } else {
            inboundEvent = this.vmsChannels.transactionInputQueue().poll();
            if(inboundEvent == null) return;
        }
        // drain all
        drained.add(inboundEvent);

        this.vmsChannels.transactionInputQueue().drainTo(drained);

        for(InboundEvent inboundEvent_ : drained){
            this.processNewEvent(inboundEvent_);
        }
        drained.clear();
//        int drained = 0;
//        do {
//            this.processNewEvent(inboundEvent);
//            drained++;
//        } while(drained < this.maxToDrain && (inboundEvent = this.vmsChannels.transactionInputQueue().poll()) != null);
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
        if(this.lastTidToTidMap.containsKey(inboundEvent.lastTid())){
            LOGGER.log(ERROR, "Inbound event is attempting to overwrite precedence of TIDs. \nOriginal last TID:" +
                    this.lastTidToTidMap.get(inboundEvent.lastTid()) + "\n Corrupt event:" + inboundEvent);
        } else {
            this.lastTidToTidMap.put(inboundEvent.lastTid(), inboundEvent.tid());
        }
    }

    public long lastTidFinished(){
        return this.lastTidFinished.get();
    }

}
