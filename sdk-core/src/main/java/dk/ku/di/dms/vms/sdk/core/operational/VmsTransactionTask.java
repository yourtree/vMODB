package dk.ku.di.dms.vms.sdk.core.operational;

import dk.ku.di.dms.vms.modb.common.event.IEvent;

/**
 * A data class that encapsulates the events
 * that form the input of a data operation.
 * In other words, the actual data operation ready for execution.
 */
public class VmsTransactionTask {

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

    @Override
    public int hashCode() {
        return this.tid;
    }

    public void putEventInput(int index, IEvent event){
        this.inputs[index] = event;
        this.remainingTasks--;
    }

    public int tid() {
        return tid;
    }

    public VmsTransactionSignature signature(){
        return signature;
    }

    public IEvent[] inputs(){
        return inputs;
    }

    public boolean isReady(){
        return remainingTasks == 0;
    }

}