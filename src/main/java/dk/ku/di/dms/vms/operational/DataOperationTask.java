package dk.ku.di.dms.vms.operational;

import dk.ku.di.dms.vms.event.IEvent;

/**
 * A data class that encapsulates the events
 * that form the input of a data operation.
 * In other words, the actual data operation ready for execution.
 */
public class DataOperationTask {

    public final DataOperationSignature signature;

    // The list must obey the order of the parameters? Can I use a set?
    public IEvent[] inputs;

    public DataOperationTask(DataOperationSignature signature, IEvent[] inputs) {
        this.signature = signature;
        this.inputs = inputs;
    }

}
