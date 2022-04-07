package dk.ku.di.dms.vms.sdk.core.operational;

import dk.ku.di.dms.vms.modb.common.event.IEvent;
import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.Callable;

/**
 * A data class that encapsulates the events
 * that form the input of a data operation.
 * In other words, the actual data operation ready for execution.
 */
public class VmsTransactionTask implements Callable<TransactionalEvent> {

    private final int tid;

    private final VmsTransactionSignature signature;

    private final IEvent[] inputs;

    private int remainingTasks;

    public VmsTransactionTask (int tid, VmsTransactionSignature signature, int inputSize){
        this.tid = tid;
        this.signature = signature;
        this.inputs = new IEvent[inputSize];
        this.remainingTasks = inputSize;
    }

    public void putEventInput(int index, IEvent event){
        this.inputs[index] = event;
        this.remainingTasks--;
    }

    public int tid() {
        return tid;
    }

    public boolean isReady(){
        return remainingTasks == 0;
    }

    private IEvent callMethod(){
        try {
            return (IEvent) signature.method().invoke(signature.vmsInstance(), inputs);
        } catch (IllegalAccessException | InvocationTargetException e) {
            // logger.info(e.getLocalizedMessage());
            throw new RuntimeException("ERROR");
        }
    }

    @Override
    public TransactionalEvent call() {

        IEvent output = callMethod();

        // if null, something went wrong, thus look at the logs
        if(output != null){
            return new TransactionalEvent( tid, signature.outputQueue(), output );
        }

        return null;

    }

}