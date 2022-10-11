package dk.ku.di.dms.vms.sdk.core.operational;

import dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionMetadata;

import java.util.concurrent.Callable;

/**
 * A class that encapsulates the events
 * that form the input of a data operation.
 * In other words, the actual data operation ready for execution.
 * FIXME this class is holding a lot of information. it must only know about tid and output object
 */
public class VmsTransactionTask implements Callable<VmsTransactionTaskResult> {

    // this is the global tid
    private final long tid;

    private final long lastTid;

    private final long batch;

    // the information necessary to run the method
    private final VmsTransactionSignature signature;

    // internal identification of this specific task in the scheduler
    // used to to allow atomic visibility)
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

    /**
     * Only set for write transactions.
     * @param identifier a task identifier among the tasks of a TID
     */
    public void setIdentifier(int identifier){
        this.identifier = identifier;
    }

    public TransactionTypeEnum getTransactionType(){
        return this.signature.type();
    }

    public long tid() {
        return this.tid;
    }

    public boolean isReady(){
        return this.remainingTasks == 0;
    }

    @Override
    public VmsTransactionTaskResult call() {

        // get thread id
        long threadId = Thread.currentThread().getId();

        // register thread in the transaction facade
        TransactionMetadata.registerTransaction(threadId, this.tid, this.identifier);

        try {

            Object output = signature.method().invoke(this.signature.vmsInstance(), this.inputs);

            // can be null, given we have terminal events (void method)
            // could also be terminal and generate event.. maybe an external system wants to consume
            // then send to the leader...
            OutboundEventResult eventOutput = new OutboundEventResult(this.tid, this.lastTid, this.batch, this.signature.outputQueue(), output, this.signature.terminal());

            // TODO we need to erase the transactions that are not seen by any more new transactions
            if(signature.type() != TransactionTypeEnum.R){
                TransactionMetadata.registerWriteTaskFinished(this.tid, this.identifier);
            }

            return new VmsTransactionTaskResult(
                    threadId,
                    this.tid,
                    this.identifier,
                    eventOutput,
                    VmsTransactionTaskResult.Status.SUCCESS);


        } catch (Exception e) {
            // (i) whether to return to the scheduler or (ii) to push to the payload handler for forwarding it to the queue
            // we can only notify it because the scheduler does not need to know the events. the scheduler just needs to
            // know whether the processing of events has been completed can be directly sent to the microservice outside
            // taskResultQueue.add(new VmsTransactionTaskResult(threadId, tid, identifier, true));
            return new VmsTransactionTaskResult(
                    threadId,
                    this.tid,
                    this.identifier,
                    null,
                    VmsTransactionTaskResult.Status.FAILURE);

        }

    }

}