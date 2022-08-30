package dk.ku.di.dms.vms.coordinator.infra;

import java.util.Collection;

/**
 * Java queue interface has many methods
 * that are not necessarily needed
 * in this project
 * @param <T>
 */
public interface SimpleQueue<T> {

    T remove();

    void add(T element);

    void drainTo(Collection<T> list);

    int size();

    boolean isEmpty();

}
