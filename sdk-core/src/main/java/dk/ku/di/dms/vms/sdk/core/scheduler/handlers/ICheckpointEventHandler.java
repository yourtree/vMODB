package dk.ku.di.dms.vms.sdk.core.scheduler.handlers;

/**
 * Must handle the reception and triggering of
 * checkpoint. Must be a mediator between the
 * event handler and the transaction scheduler
 * to avoid coupling these two concepts
 */
public interface ICheckpointEventHandler {

    /**
     * Ideally should run in a caller's thread
     */
    void checkpoint();

    boolean mustCheckpoint();

}
