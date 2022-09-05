package dk.ku.di.dms.vms.modb.api.interfaces;

/**
 * Only for testing distributed checkpointing algorithm, not for exposing as a client API
 * @param <T>
 */
public interface IVMsFutureCancellable<T> extends IVmsFuture<T> {

    boolean isCancelled();

    T get(long timeout);

}
