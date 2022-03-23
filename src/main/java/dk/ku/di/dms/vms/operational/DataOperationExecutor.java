package dk.ku.di.dms.vms.operational;

import dk.ku.di.dms.vms.event.IEvent;
import dk.ku.di.dms.vms.event.EventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;

/**
 * The responsible for actually executing a data operation task
 *
 * TODO in the future, a scheduled thread pool executor may suffice to save CPU cycles
 */
public final class DataOperationExecutor implements Runnable  {

    private static Logger logger = LoggerFactory.getLogger(DataOperationExecutor.class);

    final private EventRepository eventRepository;

    public DataOperationExecutor(EventRepository eventRepository){
        this.eventRepository = eventRepository;
    }

    // by design may not need coordination
    // but have to reason about it. given event requires another event?
    // are these threads talking to each other? the less, the better

    // TODO see http://www.h2database.com/html/advanced.html#two_phase_commit

    @Override
    public void run() {

        while(true){
            // input queue handler
            while(!eventRepository.readyQueue.isEmpty()) {
                DataOperationTask task = eventRepository.readyQueue.poll();
                DataOperationSignature dataOperation = task.signature;
                IEvent[] input = task.inputs;
                try {
                    IEvent output = null;

                    output  = (IEvent) dataOperation.method.invoke(
                                dataOperation.methodClazz,
                                input);

                    if(output != null){
                        eventRepository.outputQueue.add(output);
                    }

                } catch (IllegalAccessException | InvocationTargetException  e) {
                    e.printStackTrace();
                }
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
