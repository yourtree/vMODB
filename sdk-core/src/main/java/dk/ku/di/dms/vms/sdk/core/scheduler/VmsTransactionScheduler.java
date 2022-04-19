package dk.ku.di.dms.vms.sdk.core.scheduler;

import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;
import dk.ku.di.dms.vms.modb.common.utils.IdentifiableNode;
import dk.ku.di.dms.vms.sdk.core.event.pubsub.IVmsInternalPubSubService;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionSignature;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTask;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTaskResult;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import static java.util.logging.Logger.GLOBAL_LOGGER_NAME;
import static java.util.logging.Logger.getLogger;

/**
 * The brain of the virtual microservice runtime
 * It consumes events, verifies whether a data operation is ready for execution
 * and dispatches them for execution. If an operation is not ready yet, given the
 * event dependencies, it is stored in a waiting list until pending events arrive
 */
public class VmsTransactionScheduler implements Runnable {

    private static final Logger logger = getLogger(GLOBAL_LOGGER_NAME);

    private final Queue<TransactionalEvent> inputQueue;

    private final Queue<TransactionalEvent> outputQueue;

    private final Queue<VmsTransactionTaskResult> resultQueue;

    private final Map<String, List<IdentifiableNode<VmsTransactionSignature>>> eventToTransactionMap;

    // event that cannot execute because some dependence need to be fulfilled
    // event A < B < C < D
    // TODO check later (after HTM impl) if I can do it with a Hash...

    // Based on the transaction id (tid), I can find the task very fast
    private final Map<Integer, List<VmsTransactionTask>> waitingTasksPerTidMap;

    // A tree map because the executor requires executing the operation in order
    private final TreeMap<Integer, List<VmsTransactionTask>> readyTasksPerTidMap;

    // offset tracking for execution
    private Offset currentOffset;

    // offset tracking. i.e., cannot issue a task if predecessor transaction is not ready yet
    private final Map<Integer, Offset> offsetMap;

    private final ExecutorService vmsTransactionTaskPool;

    // TODO maybe change to atomic integer?
    private final AtomicBoolean stopSignal;

    public VmsTransactionScheduler(ExecutorService vmsTransactionTaskPool,
                                   IVmsInternalPubSubService vmsInternalPubSubService,
                                   Map<String, List<IdentifiableNode<VmsTransactionSignature>>> eventToTransactionMap){

        this.vmsTransactionTaskPool = vmsTransactionTaskPool;

        this.eventToTransactionMap = eventToTransactionMap;

        this.waitingTasksPerTidMap = new HashMap<>();

        this.readyTasksPerTidMap = new TreeMap<>();

        this.offsetMap = new TreeMap<>();

        this.stopSignal = new AtomicBoolean(false);

        this.inputQueue = vmsInternalPubSubService.inputQueue();

        this.resultQueue = vmsInternalPubSubService.resultQueue();

        this.outputQueue = vmsInternalPubSubService.outputQueue();

    }

    private TransactionalEvent take(){
        if(inputQueue.size() > 0) return inputQueue.poll();
        return null;
    }

    private void initializeOffset(){
        currentOffset = new Offset(0, 1);
        currentOffset.signalReady();
        currentOffset.signalFinished();
        offsetMap.put(0, currentOffset);
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
     */
    @Override
    public void run() {

        initializeOffset();

        while(!isStopped()) {

            processNewEvent();

            // let's dispatch all the events ready
            dispatchReadyTasksForExecution();

            processTaskResult();

            // why do we have another move offset here?
            // in case we start (or restart the VM service), we need to move the pointer only when it is safe
            // we cannot position the offset to the actual next, because we may not have received the next event yet
            moveOffsetPointerIfNecessary();

        }

    }

    private void processTaskResult() {

        while(!resultQueue.isEmpty()){

            VmsTransactionTaskResult res = resultQueue.poll();

            if(!res.failed()){

                currentOffset.signalFinished();

                if(currentOffset.status() == Offset.OffsetStatus.FINISHED){

                    // clean maps
                    readyTasksPerTidMap.remove( currentOffset.tid() );

                }

            } // else --> should deal with abort...

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
        Offset offset = offsetMap.get( task.tid() );

        offset.signalReady();

    }

    private void processNewEvent(){

        TransactionalEvent transactionalEvent = take();

        // in case there is a new event to process
        if(transactionalEvent == null) {
            return;
        }

        // have I created the task already?
        // in other words, a previous event for the same tid have been processed?
        if(waitingTasksPerTidMap.containsKey(transactionalEvent.tid())){

            // add
            List<VmsTransactionTask> notReadyTasks = waitingTasksPerTidMap.get( transactionalEvent.tid() );

            List<IdentifiableNode<VmsTransactionSignature>> signatures = eventToTransactionMap.get(transactionalEvent.queue());

            List<Integer> toRemoveList = new ArrayList<>();

            VmsTransactionTask task;
            for( int i = 0; i < notReadyTasks.size(); i++ ){

                task = notReadyTasks.get(i);
                // if here means the exact parameter position
                task.putEventInput( signatures.get(i).id(), transactionalEvent.event() );

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

            // create and put in the event list

            List<IdentifiableNode<VmsTransactionSignature>> signatures = eventToTransactionMap.get(transactionalEvent.queue());

            // create the offset
            offsetMap.put( transactionalEvent.tid(), new Offset(transactionalEvent.tid(), signatures.size()));

            VmsTransactionTask task;

            for (IdentifiableNode<VmsTransactionSignature> vmsTransactionSignatureIdentifiableNode : signatures) {

                VmsTransactionSignature signature = vmsTransactionSignatureIdentifiableNode.object();
                task = new VmsTransactionTask(
                        transactionalEvent.tid(),
                        vmsTransactionSignatureIdentifiableNode.object(),
                        signature.inputQueues().length,
                        outputQueue,
                        resultQueue
                        );

                task.putEventInput(vmsTransactionSignatureIdentifiableNode.id(), transactionalEvent.event());

                // in case only one event
                if (task.isReady()) {
                    handleNewReadyTask( task );
                }

            }

        }

    }


    private void dispatchReadyTasksForExecution() {

        if(!readyTasksPerTidMap.isEmpty() && readyTasksPerTidMap.firstKey() == currentOffset.tid() && currentOffset.status() == Offset.OffsetStatus.READY){

            List<VmsTransactionTask> tasks = readyTasksPerTidMap.get(readyTasksPerTidMap.firstKey());

            // later, we may have precedence between tasks of the same tid
            // i.e., right now any ordering is fine
            int idx = 0;
            for( VmsTransactionTask task : tasks ){
                task.setIdentifier( idx ); // arbitrary unique identifier
                idx++;
                // submit
                vmsTransactionTaskPool.submit( task );
            }

            currentOffset.moveToExecutingState();

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
    private void moveOffsetPointerIfNecessary(){

        // if( offsetMap.size() == 1 ) return; // cannot move, the event hasn't arrived yet, so the respective offset has not been created

        // if next is the right one ---> the concept of "next" may change according to recovery from failures and aborts
        if(currentOffset.status() == Offset.OffsetStatus.FINISHED && offsetMap.get( currentOffset.tid() + 1 ) != null ){

            // should be here to remove the tid 0. the tid 0 never receives a result task
            offsetMap.remove( currentOffset.tid() );

            currentOffset = offsetMap.get( currentOffset.tid() + 1 );
        }

    }

    public void stop(){
        this.stopSignal.set(true);
        // TODO dereference every internal data structure, perhaps using synchronized
    }

    private boolean isStopped() {
        return this.stopSignal.get();
    }

}
