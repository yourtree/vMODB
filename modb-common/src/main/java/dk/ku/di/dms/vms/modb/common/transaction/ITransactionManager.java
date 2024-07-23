package dk.ku.di.dms.vms.modb.common.transaction;

/**
 * Interface to which client classes (i.e., event handler) can request a checkpoint of the state
 */
public interface ITransactionManager {

    void checkpoint(long maxTid);

    void commit();

    void beginTransaction(long tid, int identifier, long lastTid, boolean readOnly);

}
