package dk.ku.di.dms.vms.scheduler;

import dk.ku.di.dms.vms.event.EventRepository;
import dk.ku.di.dms.vms.event.TransactionalEvent;
import dk.ku.di.dms.vms.operational.DataOperationSignature;
import dk.ku.di.dms.vms.operational.DataOperationTask;
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

    private static Logger logger = LoggerFactory.getLogger(Scheduler.class);

    private final EventRepository eventRepository;

    public final Map<String, List<DataOperationSignature>> eventToOperationMap;

    // event that cannot execute because some dependence need to be fulfilled
    // event A < B < C < D
    // TODO check later (after HTM impl) if I can do it with a Hash...

    // Based on the transaction id (tid), I can find the task very fast
    public final Map<Integer,List<DataOperationTask>> waitingList;

    // A queue because the executor requires executing the operation in order
    public final Queue<DataOperationTask> readyList;

    public Scheduler(
            EventRepository eventRepository,
            Map<String, List<DataOperationSignature>> eventToOperationMap){

        this.eventRepository = eventRepository;

        this.eventToOperationMap = eventToOperationMap;

        // no concurrency, only one thread operating over it
        this.waitingList = new HashMap<Integer,List<DataOperationTask>>();

        // concurrency required, two threads operating over it
        this.readyList = new ConcurrentLinkedQueue<>();

    }

    @Override
    public void run() {
        // TODO for each queue

        int offset = 0;

        while (true) {

            // input queue handler
            while (!eventRepository.inputQueue.isEmpty()) {
                TransactionalEvent event = (TransactionalEvent) eventRepository.inputQueue.poll();
                //if(event != null) {
                // TODO verify if it is safe to add to readyList of events to be processed

                if (event.tid > offset) {
                    // TODO make add sorted ,then later seek O(1)
                    // waitingList.add(event);
                    continue;
                }

                // readyList.add(event);
                //}
            }

            try {
                // TODO use wait();
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}
