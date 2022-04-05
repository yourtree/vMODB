package dk.ku.di.dms.vms.sdk.core.operational;

import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;
import dk.ku.di.dms.vms.modb.common.event.IEvent;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;
import java.util.logging.Logger;

import static java.util.logging.Logger.GLOBAL_LOGGER_NAME;
import static java.util.logging.Logger.getLogger;

/**
 * The responsible for actually executing a data operation task
 *
 * TODO in the future, a scheduled thread pool executor may suffice to save CPU cycles
 */
public final class VmsTransactionExecutor implements Supplier<TransactionalEvent> {

    //private static final Logger logger = getLogger(GLOBAL_LOGGER_NAME);

    private final VmsTransactionTask task;

    public VmsTransactionExecutor(final VmsTransactionTask task){
        this.task = task;
    }

    // by design may not need coordination
    // but have to reason about it. given event requires another event?
    // are these threads talking to each other? the less, the better

    // TODO see http://www.h2database.com/html/advanced.html#two_phase_commit

    @Override
    public TransactionalEvent get() {

        VmsTransactionSignature vmsTransactionSignature = task.signature();
        IEvent[] inputEvents = task.inputs();
        try {
            IEvent output = (IEvent) vmsTransactionSignature.method().invoke(
                        vmsTransactionSignature.vmsInstance(), inputEvents);

            // if null, something went wrong, thus look at the logs
            if(output != null){
                return new TransactionalEvent( task.tid(), vmsTransactionSignature.outputQueue(), output );
            }

        } catch (IllegalAccessException | InvocationTargetException  e) {
            //logger.info(e.getLocalizedMessage());
        }

        return null;

    }

}