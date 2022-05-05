package dk.ku.di.dms.vms.modb.common.interfaces;

/**
 *
 * @param <T>
 */
public interface IVmsFuture<T> {

    T get();

    boolean isDone();

}
