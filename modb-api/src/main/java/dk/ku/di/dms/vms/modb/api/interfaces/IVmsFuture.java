package dk.ku.di.dms.vms.modb.api.interfaces;

/**
 *
 * @param <T>
 */
public interface IVmsFuture<T> {

    T get();

    boolean isDone();

}
