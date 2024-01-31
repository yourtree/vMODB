package dk.ku.di.dms.vms.modb.storage.iterator;

import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import jdk.internal.misc.Unsafe;

import java.util.Iterator;

public interface IRecordIterator<K> extends Iterator<K> {

    Unsafe UNSAFE = MemoryUtils.UNSAFE;

    // current address
    default long address() {
        throw new IllegalStateException("This iterator implementation does not provide access to memory address.");
    }

}
