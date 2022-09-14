package dk.ku.di.dms.vms.modb.common.data_structure;

import java.util.Collection;

/**
 * Use CAS operation for producers, on writing to the memory data structures.
 * Lock is used only if consumer and producer access the head.
 * @param <T>
 */
public class MultipleProducersSingleConsumerQueue<T> implements SimpleQueue<T> {

    // similar to two phase commit. avoid synchronization overhead

    // if the number of writers is known beforehand, it is easier.
    // assign buckets. if bucket is complete, read and then recycle
    // partitioning the data for parallel writes

    // but arbitrary number requires monitor

    // strategy: manage the tail with atomic integer like jdk.internal.net.http.websocket.MessageQueue

    @Override
    public T remove() {
        return null;
    }

    @SafeVarargs
    public final void addAll(T... elements) {
        // batch processing to alleviate synchronization
    }

    @Override
    public void add(T element) {

    }

    @Override
    public void drainTo(Collection<T> list) {

    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

}
