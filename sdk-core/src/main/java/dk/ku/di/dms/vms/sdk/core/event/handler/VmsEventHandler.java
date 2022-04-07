package dk.ku.di.dms.vms.sdk.core.event.handler;

import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;

import java.util.Queue;

/**
 * Simply a facade between the external world (e.g., concrete web client
 * implementations) and the internal abstractions (i.e., inputQueue)
 */
public record VmsEventHandler(
        Queue<TransactionalEvent> inputQueue) implements IVmsEventHandler {

    @Override
    public void accept(final TransactionalEvent transactionalEvent) {
        inputQueue.add(transactionalEvent);
    }

}
