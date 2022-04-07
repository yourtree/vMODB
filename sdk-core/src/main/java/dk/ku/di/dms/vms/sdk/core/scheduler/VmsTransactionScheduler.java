package dk.ku.di.dms.vms.sdk.core.scheduler;

import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;
import dk.ku.di.dms.vms.modb.common.utils.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.utils.OrderedList;
import dk.ku.di.dms.vms.sdk.core.event.IVmsInternalPubSubService;
import dk.ku.di.dms.vms.sdk.core.event.VmsInternalPubSub;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionExecutor;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionSignature;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTask;

import java.util.*;
import java.util.concurrent.*;
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

    private final IVmsInternalPubSubService vmsInternalPubSub;

    private final Map<String, List<IdentifiableNode<VmsTransactionSignature>>> eventToTransactionMap;

    // event that cannot execute because some dependence need to be fulfilled
    // event A < B < C < D
    // TODO check later (after HTM impl) if I can do it with a Hash...

    // Based on the transaction id (tid), I can find the task very fast
    private final Map<Integer, List<VmsTransactionTask>> waitingTasksPerTidMap;

    // A queue because the executor requires executing the operation in order
    private final OrderedList<Integer, VmsTransactionTask> readyList;

    // offset tracking for execution
    private Offset offset;

    // offset tracking. i.e., cannot issue a task if predecessor transaction is not ready yet
    private final OrderedList<Integer, Offset> offsetList;

    private final ExecutorService vmsTransactionTaskPool;

    private final List<Future<TransactionalEvent>> submittedTasksList;

    private final CountDownLatch stopSignalLatch;

    public VmsTransactionScheduler(
            final ExecutorService vmsTransactionTaskPool,
            final VmsInternalPubSub vmsInternalPubSub,
            final Map<String, List<IdentifiableNode<VmsTransactionSignature>>> eventToTransactionMap){

        this.vmsTransactionTaskPool = vmsTransactionTaskPool;

        this.vmsInternalPubSub = vmsInternalPubSub;

        this.eventToTransactionMap = eventToTransactionMap;

        this.waitingTasksPerTidMap = new HashMap<>();

        this.readyList = new OrderedList<>(VmsTransactionTask::tid, Integer::compareTo);

        this.offsetList = new OrderedList<>(Offset::tid, Integer::compareTo);

        // TODO check whether this is the best implementation for synchronization
        this.submittedTasksList = new ArrayList<>();

        this.stopSignalLatch = new CountDownLatch(1);

    }

    private static class Offset {

        private enum OffsetStatus {
            READY,
            DONE
        }

        private final int tid;
        private int remainingTasks;
        private OffsetStatus status;

        public Offset(int tid, int remainingTasks) {
            this.tid = tid;
            this.remainingTasks = remainingTasks;
            this.status = OffsetStatus.READY;
        }

        private void moveToDoneState(){
            this.status = OffsetStatus.DONE;
        }

        // constraints in objects would be great! e.g., remainingTasks >= 0 always
        public void decrease(){
            if(this.remainingTasks == 0) throw new RuntimeException("Cannot have below zero remaining tasks.");
            this.remainingTasks--;
            if(remainingTasks == 0) moveToDoneState();
        }

        public int tid() {
            return this.tid;
        }
    }

    private TransactionalEvent take(){
        try {
            if(vmsInternalPubSub.inputQueue().size() > 0)
                return vmsInternalPubSub.inputQueue().take();
        } catch (InterruptedException e) {
            logger.warning("Taking event from input queue has failed.");
        }
        return null;
    }

    private void initializeOffset(){
        offset = new Offset(0, 0);
        offset.moveToDoneState();
        offsetList.addLikelyHeader(offset);
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

        TransactionalEvent transactionalEvent = take();

        while(!isStopped()) {

            processNewEvent(transactionalEvent);

            // let's dispatch all the events ready
            dispatchReadyTasksForExecution();

            // let's dispatch all output events
            dispatchOutputEvents();

            // why do we have another move offset here?
            // in case we start (or restart the VM service), we need to move the pointer only when it is safe
            // we cannot position the offset to the actual next, because we may not have received the next event yet
            moveOffsetPointerIfNecessary();

            // get bew event if any has arrived
            transactionalEvent = take();

        }

    }

    private void dispatchOutputEvents() {

        Iterator<Future<TransactionalEvent>> iterator = submittedTasksList.iterator();
        while(iterator.hasNext()){
            Future<TransactionalEvent> task = iterator.next();
            if(task.isDone()){
                try{
                    TransactionalEvent outputEvent = task.get();
                    vmsInternalPubSub.outputQueue().add(outputEvent);
                    iterator.remove();
                } catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }

    }

    private void processNewEvent(final TransactionalEvent transactionalEvent){

        // in case there is a new event to process
        if(transactionalEvent != null){

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

                        int idx = 0;
                        while(readyList.get( idx ).tid() < task.tid()){
                            idx++;
                        }
                        readyList.add( idx, task );

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

                VmsTransactionTask task;
                for (IdentifiableNode<VmsTransactionSignature> vmsTransactionSignatureIdentifiableNode : signatures) {

                    VmsTransactionSignature signature = vmsTransactionSignatureIdentifiableNode.object();
                    task = new VmsTransactionTask(transactionalEvent.tid(), vmsTransactionSignatureIdentifiableNode.object(), signature.inputQueues().length);

                    task.putEventInput(vmsTransactionSignatureIdentifiableNode.id(), transactionalEvent.event());

                    // in case only one event
                    if (task.isReady()) {
                        readyList.addLikelyHeader( task );
                    }

                }

                // now it is time to create the offset
                offsetList.addLikelyHeader(new Offset(transactionalEvent.tid(), signatures.size()));

            }

        }

    }

    private void dispatchReadyTasksForExecution() {

        while(!readyList.isEmpty() && readyList.get(0).tid() == offset.tid){

            VmsTransactionTask task = readyList.get(0);
            readyList.remove(0);

            // submit
            submittedTasksList.add( vmsTransactionTaskPool.submit( new VmsTransactionExecutor(task) ) );

            offset.decrease();

            // we drain the readyList as much a possible
            moveOffsetPointerIfNecessary();

        }

    }

    /**
     * Assumption: we always have at least one offset in the list. of course, I could do this by design but the code guarantee that
     * Is it safe to move the offset pointer? this method takes care of that
     */
    private void moveOffsetPointerIfNecessary(){

        if( offsetList.size() == 1 ) return; // cannot move, the event hasn't arrived yet, so the respective offset has not been created

        // if next is the right one
        if( offset.status == Offset.OffsetStatus.DONE && offsetList.get(1).tid == offset.tid + 1 ){
            offsetList.remove(0);
            offset = offsetList.get(0);
        }

    }

    public void stop(){
        this.stopSignalLatch.countDown();
        // TODO dereference every internal data structure, perhaps using synchronized
    }

    private boolean isStopped() {
        return this.stopSignalLatch.getCount() == 0;
    }

}
