package dk.ku.di.dms.vms.sdk.core.scheduler;

import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;
import dk.ku.di.dms.vms.sdk.core.event.EventChannel;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionSignature;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The brain of the virtual microservice runtime
 * It collects events, verifies whether a data operation is ready for execution
 * and dispatches them for execution. If an operation is not ready yet given
 * event dependencies, it is stored in a waiting list until pending events arrive
 */
public class Scheduler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);

    private final EventChannel eventChannel;

    public final Map<String, List<VmsTransactionSignature>> eventToOperationMap;

    // event that cannot execute because some dependence need to be fulfilled
    // event A < B < C < D
    // TODO check later (after HTM impl) if I can do it with a Hash...

    // Based on the transaction id (tid), I can find the task very fast
    public final Map<Integer,List<VmsTransactionTask>> waitingList;

    // A queue because the executor requires executing the operation in order
    public final Queue<VmsTransactionTask> readyList;

    public Scheduler(
            EventChannel eventChannel,
            Map<String, List<VmsTransactionSignature>> eventToOperationMap){

        this.eventChannel = eventChannel;

        this.eventToOperationMap = eventToOperationMap;

        // no concurrency, only one thread operating over it
        this.waitingList = new HashMap<>();

        // concurrency required, two threads operating over it
        this.readyList = new ConcurrentLinkedQueue<>();

    }

    @Override
    public void run() {
        // TODO for each queue

        int offset = 0;

        while (true) {

            // input queue handler
            while (!eventChannel.inputQueue.isEmpty()) {
                TransactionalEvent event = eventChannel.inputQueue.poll();
                //if(event != null) {
                // TODO verify if it is safe to add to readyList of events to be processed

                if (event.tid() > offset) {
                    // TODO make add sorted ,then later seek O(1)
                    // waitingList.add(event);
                    //continue;
                }

                // readyList.add(event);
                //}
            }

//            try {
//                // TODO use wait();
//                // Thread.sleep(2000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }

        }
    }
}
