package dk.ku.di.dms.vms.sdk.core.operational;

import dk.ku.di.dms.vms.sdk.core.event.EventChannel;
import dk.ku.di.dms.vms.modb.common.event.IEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;

/**
 * The responsible for actually executing a data operation task
 *
 * TODO in the future, a scheduled thread pool executor may suffice to save CPU cycles
 */
public final class VmsTransactionExecutor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(VmsTransactionExecutor.class);

    final private EventChannel eventChannel;

    public VmsTransactionExecutor(final EventChannel eventChannel){
        this.eventChannel = eventChannel;
    }

    // by design may not need coordination
    // but have to reason about it. given event requires another event?
    // are these threads talking to each other? the less, the better

    // TODO see http://www.h2database.com/html/advanced.html#two_phase_commit

    @Override
    public void run() {

        while(true){
            // input queue handler
            while(!eventChannel.readyQueue.isEmpty()) {
                VmsTransactionTask task = eventChannel.readyQueue.poll();
                VmsTransactionSignature dataOperation = task.signature;
                IEvent[] inputEvents = task.inputs;
                try {
                    IEvent output = (IEvent) dataOperation.method().invoke(
                                dataOperation.vmsInstance(),
                            inputEvents);

                    if(output != null){
                        eventChannel.outputQueue.add(output);
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
