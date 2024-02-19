package dk.ku.di.dms.vms.sdk.core.scheduler;

public interface ITransactionalHandler {

    /**
     * Ideally should run in a caller's thread
     */
    void checkpoint();

    boolean mustCheckpoint();

    void commit();

    void beginTransaction(long tid, int identifier, long lastTid, boolean readOnly);

}
