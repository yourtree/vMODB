package dk.ku.di.dms.vms.sdk.core.scheduler;

import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;
import dk.ku.di.dms.vms.modb.common.utils.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.utils.OrderedList;
import dk.ku.di.dms.vms.sdk.core.event.InternalPubSub;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionExecutor;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionSignature;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTask;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.logging.Logger;

import static java.util.logging.Logger.GLOBAL_LOGGER_NAME;
import static java.util.logging.Logger.getLogger;

/**
 * The brain of the virtual microservice runtime
 * It consumes events, verifies whether a data operation is ready for execution
 * and dispatches them for execution. If an operation is not ready yet given
 * event dependencies, it is stored in a waiting list until pending events arrive
 */
public class VmsTransactionScheduler implements Runnable {

    private static final Logger logger = getLogger(GLOBAL_LOGGER_NAME);

    private final InternalPubSub internalPubSub;

    public final Map<String, List<IdentifiableNode<VmsTransactionSignature>>> eventToTransactionMap;

    // event that cannot execute because some dependence need to be fulfilled
    // event A < B < C < D
    // TODO check later (after HTM impl) if I can do it with a Hash...

    // Based on the transaction id (tid), I can find the task very fast
    public final Map<Integer,List<VmsTransactionTask>> waitingList;

    // A queue because the executor requires executing the operation in order
    public final List<VmsTransactionTask> readyList;

    // offset tracking. i.e., cannot issue a task if predecessor transaction is not ready yet
    public final OrderedList<Integer,Offset> offsetList;

    public final ExecutorService vmsTransactionTaskPool;

    public final List<Future<TransactionalEvent>> submittedTasksList;

    public VmsTransactionScheduler(
            final ExecutorService vmsTransactionTaskPool,
            final InternalPubSub internalPubSub,
            final Map<String, List<IdentifiableNode<VmsTransactionSignature>>> eventToTransactionMap){

        this.vmsTransactionTaskPool = vmsTransactionTaskPool;

        this.internalPubSub = internalPubSub;

        this.eventToTransactionMap = eventToTransactionMap;

        this.waitingList = new HashMap<>();

        this.readyList = new ArrayList<>();

        this.offsetList = new OrderedList<>(new OffsetTidFunction(), Integer::compareTo );

        this.submittedTasksList = new ArrayList<>();

    }

    private static class OffsetTidFunction implements Function<Offset,Integer>{
        @Override
        public Integer apply(Offset offset) {
            return offset.tid;
        }
    }

    private enum OffsetStatus {
        READY,
        DONE
    }

    private static class Offset {
        public final int tid;
        public int remainingTasks;
        public OffsetStatus status;

        public Offset(int tid, int remainingTasks) {
            this.tid = tid;
            this.remainingTasks = remainingTasks;
            this.status = OffsetStatus.READY;
        }

        private void moveToDoneState(){
            this.status = OffsetStatus.DONE;
        }

        // constraints in objects would be great! remainingTasks >= 0
        public void decrease(){
            if(this.remainingTasks == 0) throw new RuntimeException("Cannot have below zero remaining tasks.");
            this.remainingTasks--;
            if(remainingTasks == 0) moveToDoneState();
        }

        @Override
        public int hashCode() {
            return this.tid;
        }
    }

    private TransactionalEvent take(){
        try {
            return internalPubSub.inputQueue.take();
        } catch (InterruptedException e) {
            logger.warning("Taking event from input queue has failed.");
        }
        return null;
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

        Offset offset = new Offset(0, 0);
        offset.moveToDoneState();
        offsetList.addLikelyHeader(offset);

        TransactionalEvent transactionalEvent = take();

        // FIXME blocking, does not allow for progress in case no new events arrive
        while(transactionalEvent != null) {

            // have I created the task already?
            // in other words, a previous event for the same tid have been processed?
            if(waitingList.containsKey(transactionalEvent.tid())){

                // add
                List<VmsTransactionTask> notReadyTasks = waitingList.get( transactionalEvent.tid() );

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
                    waitingList.remove( transactionalEvent.tid() );
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

                        if(readyList.isEmpty()) {
                            readyList.add(task);
                        } else {
                            int idx = 0;
                            while (readyList.get(idx).tid() < task.tid()) {
                                idx++;
                            }
                            readyList.add(idx, task);
                        }
                    }

                }

                // now it is time to create the offset
                offsetList.addLikelyHeader(new Offset(transactionalEvent.tid(), signatures.size()));

            }

            // let's dispatch all the events ready
            while(!readyList.isEmpty() && readyList.get(0).tid() == offset.tid){

                VmsTransactionTask task = readyList.get(0);
                readyList.remove(0);

                // submit
                submittedTasksList.add( vmsTransactionTaskPool.submit( new VmsTransactionExecutor(task) ) );

                offset.decrease();

                // if there is only one task for this tid
//                if(offset.status == OffsetStatus.DONE){
//                    offsetList._removeLikelyHeader(offset);
//                    // offset advances
//
//                }
                moveOffsetPointerIfNecessary(offset);

            }

            // should make sure that all tasks concerning a tid are executed before moving to the next tid
            // when is safe to move the offset? when all tasks for the given tid are executed
            // is the next on ready list the actual next task for submission to processing?
//            if( readyList.get(0).tid() == offset.tid + 1 &&
//                    offset.status == OffsetStatus.DONE &&
//                    !offsetList.isEmpty() &&
//                    offsetList.get(0).tid == offset.tid + 1 ){
//
//                // offset receives the next
//                offset = offsetList.get(0);
//
//            } // else, we need to wait for the events

            // is it safe to move the offset pointer?
            moveOffsetPointerIfNecessary(offset);


            transactionalEvent = take();

        }


    }

    private void moveOffsetPointerIfNecessary(Offset offset){

        if( offsetList.size() == 1 ) return; // cannot move, the event hasn't arrived yet, so the respective offset has not been created

        // we always have at least one offset in the list. of course, I can do this by design but the code guarantee that

        // if next is the right one
        if( offset.status == OffsetStatus.DONE && offsetList.get(1).tid == offset.tid + 1 ){
            offsetList.remove(0);
            offset = offsetList.get(0);
        }

    }

}
