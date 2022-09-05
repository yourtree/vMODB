package dk.ku.di.dms.vms.sdk.core.operational;

import dk.ku.di.dms.vms.modb.common.transaction.TransactionMetadata;
import dk.ku.di.dms.vms.sdk.core.event.channel.IVmsInternalChannels;

import java.lang.reflect.InvocationTargetException;
import java.util.Queue;

/**
 * A class that encapsulates the events
 * that form the input of a data operation.
 * In other words, the actual data operation ready for execution.
 */
public class VmsTransactionTask implements Runnable {

    // this is the global tid
    private final long tid;

    // the information necessary to run the method
    private final VmsTransactionSignature signature;

    // internal identification of this specific task in the scheduler
    // (can later be used to specify ordering criteria between tasks)
    private int identifier;

    private final Object[] inputs;

    private int remainingTasks;

    private final Queue<OutboundEventResult> outputQueue;

    private final Queue<VmsTransactionTaskResult> resultQueue;

    public VmsTransactionTask (long tid, VmsTransactionSignature signature, int inputSize,
                               IVmsInternalChannels vmsInternalChannels){
        this.tid = tid;
        this.signature = signature;
        this.inputs = new Object[inputSize];
        this.remainingTasks = inputSize;

        this.outputQueue = vmsInternalChannels.transactionOutputQueue();
        this.resultQueue = vmsInternalChannels.transactionResultQueue();
    }

    public void putEventInput(int index, Object event){
        this.inputs[index] = event;
        this.remainingTasks--;
    }

    public long tid() {
        return tid;
    }

    public void setIdentifier(int identifier) {
        this.identifier = identifier;
    }

    public boolean isReady(){
        return remainingTasks == 0;
    }

    @Override
    public void run() {

        // get thread id
        long threadId = Thread.currentThread().getId();

        // register in the transaction facade
        TransactionMetadata.registerTransaction(threadId, tid);

        try {

            Object output = signature.method().invoke(signature.vmsInstance(), inputs);

            // can be null, given we have terminal events (void method)
            if (output != null) {

                OutboundEventResult eventOutput = new OutboundEventResult(tid, signature.outputQueue(), output);

                // push to subscriber ---> this should not be sent to external
                // world if one of the correlated task has failed
                outputQueue.add(eventOutput); // same thread

            }

            // push result to the scheduler instead of returning an object
            resultQueue.add(new VmsTransactionTaskResult(threadId, tid, identifier, false));

        } catch (IllegalAccessException | InvocationTargetException e) {

            // (i) whether to return to the scheduler or (ii) to push to the payload handler for forwarding it to the queue
            // we can only notify it because the scheduler does not need to know the events. the scheduler just needs to
            // know whether the processing of events has been completed can be directly sent to the microservice outside
            resultQueue.add(new VmsTransactionTaskResult(threadId, tid, identifier, true));
        }

    }

}