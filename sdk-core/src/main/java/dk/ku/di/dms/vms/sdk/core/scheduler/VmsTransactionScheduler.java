package dk.ku.di.dms.vms.sdk.core.scheduler;

import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;
import dk.ku.di.dms.vms.modb.common.utils.IdentifiableNode;
import dk.ku.di.dms.vms.sdk.core.event.InternalPubSub;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionSignature;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTask;

import java.util.*;
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
    public final Map<int,List<VmsTransactionTask>> waitingList;

    // A queue because the executor requires executing the operation in order
    public final List<VmsTransactionTask> readyList;

    public VmsTransactionScheduler(
            final InternalPubSub internalPubSub,
            final Map<String, List<IdentifiableNode<VmsTransactionSignature>>> eventToTransactionMap){

        this.internalPubSub = internalPubSub;

        this.eventToTransactionMap = eventToTransactionMap;

        this.waitingList = new HashMap<int,List<VmsTransactionTask>>();

        this.readyList = new ArrayList<>();

//        public VmsTransactionExecutor executor;
//        public Future<?> executorFuture;
//        public ExecutorService executorService;

    }

    private class Offset {
        public int tid;
        public int remainingTasks;
        public Offset(int tid, int remainingTasks) {
            this.tid = tid;
            this.remainingTasks = remainingTasks;
        }
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

        Offset offset = new Offset(0,0);
        TransactionalEvent transactionalEvent;

        while (true) {

            try {

                transactionalEvent = internalPubSub.inputQueue.take();

                // have I created the task already?
                // in other words, a previous event for the same tid have been processed?
                if(waitingList.containsKey(transactionalEvent.tid())){

                    // add
                    List<VmsTransactionTask> notReadyTasks = waitingList.get( transactionalEvent.tid() );

                    List<IdentifiableNode<VmsTransactionSignature>> signatures = eventToTransactionMap.get(transactionalEvent.queue());

                    List<int> toRemoveList = new ArrayList<int>();

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




                }

                // TODO should make sure that all tasks concerning a tid are executed before moving to the next tid
                // when is safe to move the offset? when all tasks for the given tid are executed


            } catch (InterruptedException e) {
                e.getStackTrace();
            }



        }

    }

}
