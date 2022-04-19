package dk.ku.di.dms.vms.sdk.core.operational;

import dk.ku.di.dms.vms.modb.common.event.IEvent;
import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;

import java.lang.reflect.InvocationTargetException;
import java.util.Queue;

/**
 * A data class that encapsulates the events
 * that form the input of a data operation.
 * In other words, the actual data operation ready for execution.
 */
public class VmsTransactionTask implements Runnable {

    // this is the global tid
    private final int tid;

    // the information
    private final VmsTransactionSignature signature;

    // internal identification of this specific task in the scheduler (can later be used to specify ordering criteria between tasks)
    private int identifier;

    private final IEvent[] inputs;

    private int remainingTasks;

    private final Queue<TransactionalEvent> outputQueue;

    private final Queue<VmsTransactionTaskResult> resultQueue;

    public VmsTransactionTask (int tid, VmsTransactionSignature signature, int inputSize,
                               Queue<TransactionalEvent> outputQueue, Queue<VmsTransactionTaskResult> resultQueue){
        this.tid = tid;
        this.signature = signature;
        this.inputs = new IEvent[inputSize];
        this.remainingTasks = inputSize;

        this.outputQueue = outputQueue;
        this.resultQueue = resultQueue;
    }

    public void putEventInput(int index, IEvent event){
        this.inputs[index] = event;
        this.remainingTasks--;
    }

    public int tid() {
        return tid;
    }

    public void setIdentifier(int identifier) {
        this.identifier = identifier;
    }

    public boolean isReady(){
        return remainingTasks == 0;
    }

    private IEvent callMethod(){
        try {
            return (IEvent) signature.method().invoke(signature.vmsInstance(), inputs);
        } catch (IllegalAccessException | InvocationTargetException e) {
            // logger.info(e.getLocalizedMessage());
            throw new RuntimeException("ERROR"); // perhaps should forward this error to scheduler? what can be used for?
        }
    }

    @Override
    public void run() {

        IEvent output = callMethod();

        // if null, something went wrong, thus look at the logs
        if(output != null){
            TransactionalEvent eventOutput = new TransactionalEvent( tid, signature.outputQueue(), output );

            //push to subscriber ---> this should not be sent to external world if one of the correlated task has failed
            outputQueue.add( eventOutput ); // same thread

            // push result to the scheduler instead of returning an object
            resultQueue.add( new VmsTransactionTaskResult( tid, identifier, false ) );

            return;
        }

        // (i) whether to return to the scheduler or (ii) to push to the event handler for forwarding it to the queue
        // we can only notify it because the scheduler does not need to know the events. the scheduler just needs to
        // know whether the processing of events has been completed can be directly sent to the microservice outside
        resultQueue.add( new VmsTransactionTaskResult( tid, identifier, true ) );

    }

}