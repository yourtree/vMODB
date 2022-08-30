package dk.ku.di.dms.vms.modb.common.interfaces.application;

/**
 *
 * @param <T>
 */
public interface IVmsFuture<T> {

    T get();

    boolean isDone();

}
