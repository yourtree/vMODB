package dk.ku.di.dms.vms.sdk.core.scheduler;

import dk.ku.di.dms.vms.modb.common.data_structure.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.sdk.core.event.channel.IVmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionSignature;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTask;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTaskResult;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static java.util.logging.Logger.getLogger;

/**
 * The brain of the virtual microservice runtime
 * It consumes events, verifies whether a data operation is ready for execution
 * and dispatches them for execution. If an operation is not ready yet, given the
 * payload dependencies, it is stored in a waiting list until pending events arrive
 */
public class VmsTransactionScheduler extends StoppableRunnable {

    protected static final Logger logger = getLogger("VmsTransactionScheduler");

    // payload that cannot execute because some dependence need to be fulfilled
    // payload A < B < C < D
    // TODO check later (after HTM impl) if I can do it with a Hash...

    // Based on the transaction id (tid), I can find the task very fast
    private final Map<Long, List<VmsTransactionTask>> waitingTasksPerTidMap;

    // A tree map because the executor requires executing the operation in order
    private final TreeMap<Long, List<VmsTransactionTask>> readyTasksPerTidMap;

    private final Map<Long, List<Future<VmsTransactionTaskResult>>> submittedTasksPerTidMap;

    private final Map<Long, List<VmsTransactionTaskResult>> resultTasksPerTidMap;

    // offset tracking for execution
    private OffsetTracker currentOffset;

    // offset tracking. i.e., cannot issue a task if predecessor transaction is not ready yet
    private final Map<Long, OffsetTracker> offsetMap;

    // map the last tid
    private final Map<Long, Long> lastTidToTidMap;

    private final ExecutorService vmsTransactionTaskPool;

    protected final IVmsInternalChannels vmsChannels;

    // mapping
    private final Map<String, List<IdentifiableNode<VmsTransactionSignature>>> eventToTransactionMap;

    private final Map<String, Class<?>> queueToEventMap;

    private final IVmsSerdesProxy serdes;

    public VmsTransactionScheduler(ExecutorService vmsAppLogicTaskPool,
                                   IVmsInternalChannels vmsChannels,
                                   Map<String, List<IdentifiableNode<VmsTransactionSignature>>> eventToTransactionMap,
                                   Map<String, Class<?>> queueToEventMap,
                                   IVmsSerdesProxy serdes){
        super();
        this.vmsTransactionTaskPool = vmsAppLogicTaskPool;
        this.eventToTransactionMap = eventToTransactionMap;
        this.waitingTasksPerTidMap = new HashMap<>();
        this.readyTasksPerTidMap = new TreeMap<>();
        this.submittedTasksPerTidMap = new HashMap<>();
        this.resultTasksPerTidMap = new HashMap<>();
        this.offsetMap = new TreeMap<>();
        this.lastTidToTidMap = new HashMap<>();
        this.vmsChannels = vmsChannels;
        this.queueToEventMap = queueToEventMap;
        this.serdes = serdes;
    }

    /**
     * Another way to implement this is make this a fine-grained task.
     * that is, a pool of available tasks for receiving and processing the
     * events instead of an infinite while loop.
     * virtual threads are a good choice: https://jdk.java.net/loom/
     * BUT, "Virtual threads help to improve the throughput of typical
     * server applications precisely because such applications consist
     * of a great number of concurrent tasks that spend much of their time waiting."
     * Which is not this case... we are not doing I/O to wait
     * But virtual threads can be beneficial to transactional tasks
     */
    @Override
    public void run() {

        logger.info("Scheduler has started.");

        initializeOffset();

        while(isRunning()) {

            processNewEvent();

            // why do we have another move offset here?
            // in case we start (or restart the VM service), we need to move the pointer only when it is safe
            // we cannot position the offset to the actual next, because we may not have received the next payload yet
            moveOffsetPointerIfNecessary();

            // let's dispatch all the events ready
            dispatchReadyTasksForExecution();

            processTaskResult();

        }

    }

//    private TransactionEvent.Payload take(){
//        if(vmsChannels.transactionInputQueue().size() > 0) return vmsChannels.transactionInputQueue().poll();
//        return null;
//    }

    protected void initializeOffset(){
        currentOffset = new OffsetTracker(0, 1);
        currentOffset.signalReady();
        currentOffset.signalFinished();
        offsetMap.put(0L, currentOffset);
        logger.info("Offset initialized");
    }

    /**
     * TODO we have to deal with failures
     *  not only container failures but also constraints being violated
     */
    protected void processTaskResult() {

        if(submittedTasksPerTidMap.get(currentOffset.tid()) == null) return;

        List<Future<VmsTransactionTaskResult>> list = submittedTasksPerTidMap.get(currentOffset.tid());
        List<VmsTransactionTaskResult> resultList = resultTasksPerTidMap.get(currentOffset.tid());

        for(int i = list.size() - 1; i >= 0; --i){

           Future<VmsTransactionTaskResult> resultFuture = list.get(i);

            if(resultFuture.isDone()){

                VmsTransactionTaskResult res;
                try {

                    res = resultFuture.get();
                    resultList.add(res);

                    if(res.status() == VmsTransactionTaskResult.Status.SUCCESS) {

                        currentOffset.signalFinished();

                        list.remove(i);

                        if (currentOffset.status() == OffsetTracker.OffsetStatus.FINISHED_SUCCESSFULLY) {

                            // clean maps
                            readyTasksPerTidMap.remove(currentOffset.tid());
                            submittedTasksPerTidMap.remove(currentOffset.tid());

                            // now can send all to output queue (coordinator)
                            boolean atLeastOneHasPayload = false;
                            for(var result : resultList) {
                                if(result.result() != null){
                                    atLeastOneHasPayload = true;
                                    vmsChannels.transactionOutputQueue().add(result.result());
                                }

                            }

                            // what if all the results of a tid are void?
                            if(!atLeastOneHasPayload){
                                vmsChannels.transactionOutputQueue().add( new OutboundEventResult(
                                        currentOffset.tid(),
                                        null,
                                        null,
                                        true // must be marked as terminal since there is no output
                                ));
                            }

                            resultTasksPerTidMap.remove(currentOffset.tid());

                        }

                    } else {
                        currentOffset.signalError();
                        // TODO must deal with errors (i.e., abort)

                        readyTasksPerTidMap.remove(currentOffset.tid());
                        submittedTasksPerTidMap.remove(currentOffset.tid());
                        resultTasksPerTidMap.remove(currentOffset.tid());

                    }

                } catch (InterruptedException | ExecutionException ignored) {
                    logger.warning("A task supposedly done returned an exception.");
                }

            }

        }

    }

    /**
     * For handling already created entries in readyTasksPerTidMap
     * @param task The ready task
     */
    private void handleNewReadyTask( VmsTransactionTask task ){

        List<VmsTransactionTask> list = readyTasksPerTidMap.computeIfAbsent(task.tid(), k -> new LinkedList<>());
        list.add(task);

        // handle respective offset
        OffsetTracker offset = offsetMap.get( task.tid() );

        offset.signalReady();

    }

    protected void processNewEvent(){

        TransactionEvent.Payload transactionalEvent; // take();
        try {
            transactionalEvent = vmsChannels.transactionInputQueue().poll(15000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return;
        }

        // in case there is a new payload to process
        if(transactionalEvent == null) {
            return;
        }

        // have I created the task already?
        // in other words, a previous payload for the same tid have been processed?
        if(waitingTasksPerTidMap.containsKey(transactionalEvent.tid())){

            // add
            List<VmsTransactionTask> notReadyTasks = waitingTasksPerTidMap.get( transactionalEvent.tid() );

            List<IdentifiableNode<VmsTransactionSignature>> signatures = eventToTransactionMap.get(transactionalEvent.event());

            List<Integer> toRemoveList = new ArrayList<>();

            VmsTransactionTask task;
            for( int i = 0; i < notReadyTasks.size(); i++ ){

                task = notReadyTasks.get(i);
                // if here means the exact parameter position

                Class<?> clazz = this.queueToEventMap.get(transactionalEvent.event());

                Object input = this.serdes.deserialize(transactionalEvent.payload(), clazz);

                task.putEventInput( signatures.get(i).id(), input );

                // check if the input is completed
                if( task.isReady() ){
                    toRemoveList.add(i);
                    handleNewReadyTask( task );
                }

            }

            // cleaning tasks
            for(int i : toRemoveList){
                notReadyTasks.remove(i);
            }

            if(notReadyTasks.isEmpty()){
                waitingTasksPerTidMap.remove( transactionalEvent.tid() );
            }

        } else {

            // create and put in the payload list

            List<IdentifiableNode<VmsTransactionSignature>> signatures = eventToTransactionMap.get(transactionalEvent.event());

            // create the offset
            this.offsetMap.put( transactionalEvent.tid(), new OffsetTracker(transactionalEvent.tid(), signatures.size()));

            // mark the last tid
            this.lastTidToTidMap.put( transactionalEvent.lastTid(), transactionalEvent.tid() );

            VmsTransactionTask task;

            for (IdentifiableNode<VmsTransactionSignature> vmsTransactionSignatureIdentifiableNode : signatures) {

                VmsTransactionSignature signature = vmsTransactionSignatureIdentifiableNode.object();
                task = new VmsTransactionTask(
                        transactionalEvent.tid(),
                        vmsTransactionSignatureIdentifiableNode.object(),
                        signature.inputQueues().length
                        );

                Class<?> clazz = this.queueToEventMap.get(transactionalEvent.event());

                Object input = this.serdes.deserialize(transactionalEvent.payload(), clazz);

                task.putEventInput(vmsTransactionSignatureIdentifiableNode.id(), input);

                // in case only one payload
                if (task.isReady()) {
                    handleNewReadyTask( task );
                }

            }

        }

    }

    protected void dispatchReadyTasksForExecution() {

        // only currentOffset is necessary being checked, the other conditions causally follows:
        // !readyTasksPerTidMap.isEmpty() && readyTasksPerTidMap.firstKey() == currentOffset.tid() &&
        if(this.currentOffset.status() == OffsetTracker.OffsetStatus.READY){

            List<VmsTransactionTask> tasks = this.readyTasksPerTidMap.get(readyTasksPerTidMap.firstKey());

            List<Future<VmsTransactionTaskResult>> list = new ArrayList<>(tasks.size());
            this.submittedTasksPerTidMap.put(currentOffset.tid(), list);
            this.resultTasksPerTidMap.put(currentOffset.tid(), new ArrayList<>(tasks.size()));

            // later, we may have precedence between tasks of the same tid
            // i.e., right now any ordering is fine
            int idx = 0;
            for( VmsTransactionTask task : tasks ){
                task.setIdentifier( idx ); // arbitrary unique identifier
                idx++;
                // submit
                list.add( this.vmsTransactionTaskPool.submit( task ) );
            }

            this.currentOffset.moveToExecutingState();

            // must store submitted tasks in case we need to re-execute.
            //          what could go wrong?
            //           (i) a constraint not being met, would need to abort
            //           (ii) lack of machine resources. can we do something in this case?


            // we drain the readyList as much as possible
            // moveOffsetPointerIfNecessary();

        }

    }

    /**
     * Assumption: we always have at least one offset in the list. of course, I could do this by design but the code guarantee that
     * Is it safe to move the offset pointer? this method takes care of that
     */
    protected void moveOffsetPointerIfNecessary(){

        // if( offsetMap.size() == 1 ) return; // cannot move, the payload hasn't arrived yet, so the respective offset has not been created

        // if next is the right one ---> the concept of "next" may change according to recovery from failures and aborts
        if(currentOffset.status() == OffsetTracker.OffsetStatus.FINISHED_SUCCESSFULLY && this.lastTidToTidMap.get( currentOffset.tid() ) != null ){

            // should be here to remove the tid 0. the tid 0 never receives a result task
            offsetMap.remove( currentOffset.tid() );

            currentOffset = offsetMap.get( this.lastTidToTidMap.get( currentOffset.tid() ) );
        }

    }

}
