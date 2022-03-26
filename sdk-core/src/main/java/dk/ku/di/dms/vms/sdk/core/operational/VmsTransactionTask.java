package dk.ku.di.dms.vms.sdk.core.operational;

import dk.ku.di.dms.vms.modb.common.event.IEvent;

/**
 * A data class that encapsulates the events
 * that form the input of a data operation.
 * In other words, the actual data operation ready for execution.
 */
public class VmsTransactionTask {

    public final VmsTransactionSignature signature;

    // TODO DOes the list must obey the order of the parameters? Can I use a set?
    public IEvent[] inputs;

    public VmsTransactionTask(VmsTransactionSignature signature, IEvent[] inputs) {
        this.signature = signature;
        this.inputs = inputs;
    }

}
