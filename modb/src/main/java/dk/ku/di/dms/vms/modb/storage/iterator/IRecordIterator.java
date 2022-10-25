package dk.ku.di.dms.vms.modb.storage.iterator;

import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import jdk.internal.misc.Unsafe;

public interface IRecordIterator<K> {

    Unsafe UNSAFE = MemoryUtils.UNSAFE;

    /**
     * Some iterators already start with a value. Although this is not the case
     * in Java original iterators, in our case it makes sense.
     * depending on the type of index, the semantic of this method differs.
     * For unique hash index, key means the unique key identifying the record
     * For non-unique hash index, key means the set of columns identifying this entry
     */
    K get();

    /**
     * This method should not come after a hasNext call
     */
    void next();

    boolean hasElement();

    // current address
    default long address() {
        throw new IllegalStateException("This iterator implementation does not provide access to memory address.");
    }

}
