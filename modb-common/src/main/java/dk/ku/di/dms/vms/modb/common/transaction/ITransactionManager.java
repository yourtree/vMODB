package dk.ku.di.dms.vms.modb.common.transaction;

/**
 * Interface to which client classes (i.e., event handler) can request a checkpoint of the state
 */
public interface ITransactionManager {

    default void checkpoint(long maxTid) { }

    default void commit() { }

    default TransactionContextBase beginTransaction(long tid, int identifier, long lastTid, boolean readOnly) { return null; }

}
