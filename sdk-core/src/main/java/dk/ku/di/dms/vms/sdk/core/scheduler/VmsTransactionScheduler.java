package dk.ku.di.dms.vms.sdk.core.scheduler;

import dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum;
import dk.ku.di.dms.vms.modb.common.data_structure.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.sdk.core.event.channel.IVmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsTransactionMetadata;
import dk.ku.di.dms.vms.sdk.core.operational.*;
import dk.ku.di.dms.vms.sdk.core.scheduler.tracking.ComplexVmsTransactionTrackingContext;
import dk.ku.di.dms.vms.sdk.core.scheduler.tracking.IVmsTransactionTrackingContext;
import dk.ku.di.dms.vms.sdk.core.scheduler.tracking.SimpleVmsTransactionTrackingContext;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * The brain of the virtual microservice runtime
 * It consumes events, verifies whether a data operation is ready for execution
 * and dispatches them for execution. If an operation is not ready yet, given the
 * payload dependencies, it is stored in a waiting list until pending events arrive
 * TODO abort management
 * must store submitted tasks in case we need to re-execute (iff not PK, FK).
 *     what could go wrong?
 *         (i) a constraint not being met, would need to abort
 *         (ii) lack of machine resources. can we do something in this case?
 */
public class VmsTransactionScheduler extends StoppableRunnable {

    protected static final Logger logger = Logger.getLogger("VmsTransactionScheduler");

    // payload that cannot execute because some dependence need to be fulfilled
    // payload A < B < C < D
    // TODO check later (after HTM impl) if I can do it with a Hash...

    // Based on the transaction id (tid), I can find the task very fast
    private final Map<Long, List<VmsTransactionTask>> waitingTasksPerTidMap;

    private final Map<Long, IVmsTransactionTrackingContext> transactionContextMap;

    // offset tracking for execution
    private OffsetTracker currentOffset;

    // offset tracking. i.e., cannot issue a task if predecessor transaction is not ready yet
    private final Map<Long, OffsetTracker> offsetMap;

    // map the last tid
    private final Map<Long, Long> lastTidToTidMap;

    /**
     * Thread pool for read-only queries
     */
    private final ExecutorService readTaskPool;

    /**
     * Thread pool for write-only and read-write queries
     */
    private final ExecutorService writeTaskPool;

    protected final IVmsInternalChannels vmsChannels;

    // mapping
    private final Map<String, VmsTransactionMetadata> transactionMetadataMap;

    private final Map<String, Class<?>> queueToEventMap;

    private final IVmsSerdesProxy serdes;

    // reuse same collection to avoid many allocations
    private final Collection<TransactionEvent.Payload> inputEvents;

    public VmsTransactionScheduler(ExecutorService readTaskPool,
                                   IVmsInternalChannels vmsChannels,
                                   // (input) queue to transactions map
                                   Map<String, VmsTransactionMetadata> transactionMetadataMap,
                                   Map<String, Class<?>> queueToEventMap,
                                   IVmsSerdesProxy serdes){
        super();

        // thread pools
        this.readTaskPool = readTaskPool;
        this.writeTaskPool = Executors.newSingleThreadExecutor();

        // infra (come from external)
        this.transactionMetadataMap = transactionMetadataMap;
        this.vmsChannels = vmsChannels;
        this.queueToEventMap = queueToEventMap;
        this.serdes = serdes;

        // operational (internal control of transactions and tasks)
        this.waitingTasksPerTidMap = new HashMap<>();
        this.transactionContextMap = new HashMap<>();
        this.offsetMap = new HashMap<>();
        this.lastTidToTidMap = new HashMap<>();

        this.inputEvents = new ArrayList<>(50);
    }

    /**
     * Another way to implement this is make this a fine-grained task.
     * that is, a pool of available tasks for receiving and processing the
     * events instead of an infinite while loop.
     * virtual threads are a good choice: <a href="https://jdk.java.net/loom/">...</a>
     * BUT, "Virtual threads help to improve the throughput of typical
     * server applications precisely because such applications consist
     * of a great number of concurrent tasks that spend much of their time waiting."
     * Which is not this case... we are not doing I/O to wait
     * But virtual threads can be beneficial to transactional tasks
     * ---------------------
     * Tasks are dispatched respecting the single-thread model for RW tasks
     * In other words, no RW tasks for the same transaction can be scheduled
     * concurrently. One at a time. Read tasks can be scheduled concurrently.
     * To avoid interleaving between tasks of the same transaction,
     * a simple strategy is now adopted:
     * (i) All read tasks are executed first. Bad for throughput
     * (ii) Look at the prescribed precedences provided by the user. Also, bad for throughput.
     * (iii) Concurrency control with abort and restarts in case of deadlock.
     *          A read transaction may be allowed to see writes installed by different tasks.
     * (iv) Have assumptions. The user may have different tasks. ACID is only provided if the
     *      tasks do not see each other writes.
     *      In other words, no concurrency control for tasks of the same TID.
     *
     */
    @Override
    public void run() {

        logger.info("Scheduler has started");

        initializeOffset();

        while(isRunning()) {

            checkForNewEvents();

            // why do we have another move offset here?
            // in case we start (or restart the VM service), we need to move the pointer only when it is safe
            // we cannot position the offset to the actual next, because we may not have received the next payload yet
            moveOffsetPointerIfNecessary();

            // an idea to optimize is to pass a completion handler to the thread
            // the task thread then update the task result list
            processTaskResult();

        }

    }

    protected void initializeOffset(){
        this.currentOffset = new OffsetTracker(0, 1);
        this.currentOffset.signalTaskFinished();
        this.offsetMap.put(0L, this.currentOffset);
        logger.info("Offset initialized");
    }

    /**
     * TODO we have to deal with failures
     *  not only container failures but also constraints being violated
     */
    protected void processTaskResult() {

        IVmsTransactionTrackingContext context = this.transactionContextMap.get( this.currentOffset.tid() );
        if( context == null ) return;

        if(context.isSimple()){
            if(context.asSimple().future == null) return;
            if(!context.asSimple().future.isDone()) return;
            // TODO optimize, stop calling as simple
            try {
                context.asSimple().result = context.asSimple().future.get();
                context.asSimple().future = null;

                if(context.asSimple().result.status() != VmsTransactionTaskResult.Status.SUCCESS) {
                    this.currentOffset.signalError();
                    return;
                }

                this.currentOffset.signalTaskFinished();

                this.vmsChannels.transactionOutputQueue().add(
                        new VmsTransactionResult(this.currentOffset.tid(), List.of(context.asSimple().result.result())) );

                this.transactionContextMap.remove(this.currentOffset.tid());

            } catch (Exception ignored){
                logger.warning("A task supposedly done returned an exception.");
            }

            return;

        }

        ComplexVmsTransactionTrackingContext txCtx = context.asComplex();
        if(txCtx.submittedTasks.isEmpty()) return;
        List<Future<VmsTransactionTaskResult>> list = txCtx.submittedTasks;

        for(int i = list.size() - 1; i >= 0; --i){

            Future<VmsTransactionTaskResult> resultFuture = list.get(i);

            if(resultFuture == null) continue;
            if(!resultFuture.isDone()) continue;

            VmsTransactionTaskResult res;
            try {

                res = resultFuture.get();
                txCtx.resultTasks.add(res);

                if(res.status() == VmsTransactionTaskResult.Status.SUCCESS) {

                    this.currentOffset.signalTaskFinished();

                    list.remove(i);

                    if (currentOffset.status() == OffsetTracker.OffsetStatus.FINISHED_SUCCESSFULLY) {

                        List<OutboundEventResult> outbounds = new ArrayList<>(txCtx.resultTasks.size());
                        for(var resultTask : txCtx.resultTasks) {
                            outbounds.add(resultTask.result());
                        }

                        // now can send all to output queue
                        this.vmsChannels.transactionOutputQueue().add(
                                new VmsTransactionResult(currentOffset.tid(), outbounds) );

                        this.transactionContextMap.remove(this.currentOffset.tid());

                    }

                } else {
                    this.currentOffset.signalError();
                    // TODO must deal with errors (i.e., abort)
                }

            } catch (InterruptedException | ExecutionException ignored) {
                logger.warning("A task supposedly done returned an exception.");
            }

        }

    }

    /**
     * If an additional semantic is required for the scheduler (e.g., cannot block because another type
     * of event apart from transaction input might arrive such as batch),
     * this method needs to be overridden and the first IF block removed
     */
    protected final void checkForNewEvents(){

        // a safe condition to block waiting is when the current offset is finished (no result tasks to be processed)
        // however, cannot block waiting for input queue because of an unknown bug
        if(this.vmsChannels.transactionInputQueue().isEmpty()){
            return;
        }

        if(this.vmsChannels.transactionInputQueue().size() == 1) {
            try {
                TransactionEvent.Payload transactionalEvent = this.vmsChannels.transactionInputQueue().take();
                this.processNewEvent(transactionalEvent);
            } catch (InterruptedException ignored) { }
            return;
        }

        this.vmsChannels.transactionInputQueue().drainTo(this.inputEvents);

        for(var input : this.inputEvents){
            this.processNewEvent(input);
        }

        this.inputEvents.clear();

    }

    private void processNewEvent(TransactionEvent.Payload transactionalEvent){
        // have I created the task already?
        // in other words, a previous payload for the same tid have been processed?
        if(this.waitingTasksPerTidMap.containsKey(transactionalEvent.tid())){
            this.processNewEventFromKnownTransaction(transactionalEvent);
        } else if (this.offsetMap.get(transactionalEvent.tid()) == null) {
            this.processNewEventFromUnknownTransaction(transactionalEvent);
        } else {
            throw new IllegalStateException("Analyze this case....");
        }
    }

    private void processNewEventFromUnknownTransaction(TransactionEvent.Payload transactionalEvent) {
        // new tid: create it and put it in the payload list

        VmsTransactionMetadata transactionMetadata = this.transactionMetadataMap.get(transactionalEvent.event());

        // create the offset tracker
        OffsetTracker offsetTracker = new OffsetTracker(transactionalEvent.tid(), transactionMetadata.signatures.size());
        this.offsetMap.put( transactionalEvent.tid(), offsetTracker );

        // mark the last tid, so we can get the next to execute when appropriate
        this.lastTidToTidMap.put( transactionalEvent.lastTid(), transactionalEvent.tid() );

        VmsTransactionTask task;
        IVmsTransactionTrackingContext txContext;

        if(transactionMetadata.signatures.size() == 1){
            txContext = new SimpleVmsTransactionTrackingContext();
        } else {
            // create the vms transaction context TODO check why ms2 not detecting write tx number
            txContext = new ComplexVmsTransactionTrackingContext(
                    transactionMetadata.numReadTasks,
                    transactionMetadata.numReadWriteTasks,
                    transactionMetadata.numWriteTasks);
        }

        this.transactionContextMap.put( transactionalEvent.tid(), txContext );

        // for each signature, create an appropriate task to run
        for (IdentifiableNode<VmsTransactionSignature> node : transactionMetadata.signatures) {

            VmsTransactionSignature signature = node.object();
            task = new VmsTransactionTask(
                    transactionalEvent.tid(),
                    transactionalEvent.lastTid(),
                    transactionalEvent.batch(),
                    node.object(),
                    signature.inputQueues().length
                    );

            Class<?> clazz = this.queueToEventMap.get(transactionalEvent.event());

            Object input = this.serdes.deserialize(transactionalEvent.payload(), clazz);

            // put the input event on the correct slot (i.e., the correct parameter position)
            task.putEventInput(node.id(), input);

            // fast path: in case only one (event) payload
            if (task.isReady()) {
                dispatchReadyTask(task, txContext);
            } else {
                // unknown transaction, then must create the entry
                var list = new ArrayList<VmsTransactionTask>(transactionMetadata.numTasksWithMoreThanOneInput);
                this.waitingTasksPerTidMap.put(task.tid(), list);
                list.add( task );
            }

        }
    }

    private void dispatchReadyTask(VmsTransactionTask task, IVmsTransactionTrackingContext txContext) {
        if(txContext.isSimple()){
            txContext.asSimple().task = task;
            dispatchReadySimpleTask(txContext.asSimple());
        } else {
            ComplexVmsTransactionTrackingContext complexCtx = txContext.asComplex();
            task.setIdentifier(complexCtx.readAndIncrementNextTaskIdentifier());
            if (task.transactionType() == TransactionTypeEnum.R) {
                complexCtx.readTasks.add(task);
            } else {
                // they are submitted FIFO
                complexCtx.writeTasks.add(task);
            }
            dispatchReadyComplexTask(txContext);
        }
    }

    private void processNewEventFromKnownTransaction(TransactionEvent.Payload transactionalEvent) {
        List<VmsTransactionTask> notReadyTasks = this.waitingTasksPerTidMap.get( transactionalEvent.tid() );

        VmsTransactionTask task;
        for( int i = notReadyTasks.size() - 1; i >= 0; i-- ){

            task = notReadyTasks.get(i);
            // if here means the exact parameter position

            Class<?> clazz = this.queueToEventMap.get(transactionalEvent.event());

            Object input = this.serdes.deserialize(transactionalEvent.payload(), clazz);

            // another way is to deliver all input events in any order and let the task resolve the order
            // but let's incur in this overhead for now

            /* method 1:
            String methodName = task.signature().method().getName();
            var optional = transactionMetadata.signatures.stream().filter(p-> p.object().method().getName().equals(methodName)).findFirst();
            if(optional.isEmpty()){
                throw new IllegalStateException("Input event not mapped correctly.");
            }
            task.putEventInput( optional.get().id(), input );
            */

            // method 2, more efficient
            // based on the input queue, find the position based on the transaction signature
            boolean found = false;
            int j;
            for(j = 0; j < task.signature().inputQueues().length; j++){
                if(task.signature().inputQueues()[j].equalsIgnoreCase(transactionalEvent.event())){
                    found = true;
                    break;
                }
            }

            if(!found){
                throw new IllegalStateException("Input event not mapped correctly.");
            }

            task.putEventInput( j, input );

            // check if the input is completed
            if(task.isReady()){
                notReadyTasks.remove(i);
                // get transaction context
                IVmsTransactionTrackingContext txContext = this.transactionContextMap.get( task.tid() );
                dispatchReadyTask(task, txContext);
            } else {
                // known transaction, can just get the entry
                var list = this.waitingTasksPerTidMap.get(task.tid());
                list.add( task );
            }

        }

        // why doing this? in the loop above it is checked whether all input events have been delivered to the task
        // if, so, the task is removed from the waiting list
        // thus, if the waiting list is empty, the entry can be removed from the map
//        if(notReadyTasks.isEmpty()){
//            this.waitingTasksPerTidMap.remove( transactionalEvent.tid() );
//        }
    }

    private void dispatchReadyReadTaskList(ComplexVmsTransactionTrackingContext txCtx){
        int index = txCtx.readTasks.size() - 1;
        while (index >= 0) {
            VmsTransactionTask task = txCtx.readTasks.get(index);
            // later, we may have precedence between tasks of the same tid
            // i.e., right now any ordering is fine
            txCtx.submittedTasks.add( this.readTaskPool.submit(task) );
            txCtx.readTasks.remove(index);
            index--;
        }
    }

    private void dispatchReadySimpleTask(SimpleVmsTransactionTrackingContext txCtx){
        if(txCtx.task.transactionType() == TransactionTypeEnum.R){
            txCtx.future = this.readTaskPool.submit(txCtx.task);
        } else {
            txCtx.future = this.writeTaskPool.submit(txCtx.task);
        }
    }

    private void dispatchReadyComplexTask(IVmsTransactionTrackingContext context) {
        ComplexVmsTransactionTrackingContext txCtx = context.asComplex();
        int numRead = txCtx.readTasks.size();
        if(numRead > 0) dispatchReadyReadTaskList( txCtx );

        if(!txCtx.writeTasks.isEmpty()) {
            txCtx.submittedTasks.add( this.writeTaskPool.submit( txCtx.writeTasks.poll() ) );
        }
    }

    /**
     * Assumption: we always have at least one offset in the list. Of course,
     * I could do this by design but the code guarantees that
     * Is it safe to move the offset pointer? this method takes care of that
     */
    protected void moveOffsetPointerIfNecessary(){

        // if next is the right one ---> the concept of "next" may change according to recovery from failures and aborts
        if(this.currentOffset.status() == OffsetTracker.OffsetStatus.FINISHED_SUCCESSFULLY
                && this.lastTidToTidMap.get( this.currentOffset.tid() ) != null ){

            var nextTid = this.offsetMap.get( this.lastTidToTidMap.get( this.currentOffset.tid() ) );

            // has the "next" arrived already?
            if(nextTid == null) return;

            // should be here to remove the tid 0. the tid 0 never receives a result task
            this.offsetMap.remove( this.currentOffset.tid() );

            // don't need anymore
            this.lastTidToTidMap.remove( this.currentOffset.tid() );

            this.waitingTasksPerTidMap.remove( this.currentOffset.tid() );

            this.currentOffset = nextTid;

        }

    }

}
