package dk.ku.di.dms.vms.sdk.core.event;

import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;

import java.util.Queue;

/**
 * Simply a facade between the external world (e.g., concrete web client
 * implementations) and the internal abstractions (i.e., inputQueue)
 */
public class VmsEventHandler implements IVmsEventHandler {

    private final Queue<TransactionalEvent> inputQueue;

    public VmsEventHandler(final Queue<TransactionalEvent> inputQueue) {
        this.inputQueue = inputQueue;
    }

    @Override
    public void accept(final TransactionalEvent transactionalEvent) {
        inputQueue.add(transactionalEvent);
    }

}
