package dk.ku.di.dms.vms.sdk.core.operational;

import dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionMetadata;
import dk.ku.di.dms.vms.sdk.core.event.channel.IVmsInternalChannels;

import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * A class that encapsulates the events
 * that form the input of a data operation.
 * In other words, the actual data operation ready for execution.
 *
 * FIXME this class is holding a lot of information. it must only know about tid and output object
 *
 */
public class VmsTransactionTask implements Callable<VmsTransactionTaskResult> {

    // this is the global tid
    private final long tid;

    private final long lastTid;

    private final long batch;

    // the information necessary to run the method
    private final VmsTransactionSignature signature;

    // internal identification of this specific task in the scheduler
    // (can later be used to specify ordering criteria between tasks)
    private int identifier;

    private final Object[] inputs;

    private int remainingTasks;

    public VmsTransactionTask (long tid, long lastTid, long batch, VmsTransactionSignature signature, int inputSize){
        this.tid = tid;
        this.lastTid = lastTid;
        this.batch = batch;
        this.signature = signature;
        this.inputs = new Object[inputSize];
        this.remainingTasks = inputSize;
    }

    public void putEventInput(int index, Object event){
        this.inputs[index] = event;
        this.remainingTasks--;
    }

    public TransactionTypeEnum getTransactionType(){
        return this.signature.type();
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
    public VmsTransactionTaskResult call() {

        // get thread id
        long threadId = Thread.currentThread().getId();

        // register in the transaction facade
        TransactionMetadata.registerTransaction(threadId, tid);

        try {

            Object output = signature.method().invoke(signature.vmsInstance(), inputs);

            // can be null, given we have terminal events (void method)
            // could also be terminal and generate event.. maybe an external system wants to consume
            // then send to the leader...
            OutboundEventResult eventOutput = new OutboundEventResult(tid, lastTid, batch, signature.outputQueue(), output, signature.terminal());

            return new VmsTransactionTaskResult(
                    threadId,
                    tid,
                    identifier,
                    eventOutput,
                    VmsTransactionTaskResult.Status.SUCCESS);


        } catch (Exception e) {
            // (i) whether to return to the scheduler or (ii) to push to the payload handler for forwarding it to the queue
            // we can only notify it because the scheduler does not need to know the events. the scheduler just needs to
            // know whether the processing of events has been completed can be directly sent to the microservice outside
            // taskResultQueue.add(new VmsTransactionTaskResult(threadId, tid, identifier, true));
            return new VmsTransactionTaskResult(
                    threadId,
                    tid,
                    identifier,
                    null,
                    VmsTransactionTaskResult.Status.FAILURE);

        }

    }

}