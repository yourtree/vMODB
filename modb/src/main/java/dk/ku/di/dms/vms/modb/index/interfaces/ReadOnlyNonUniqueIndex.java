package dk.ku.di.dms.vms.modb.index.interfaces;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

import java.util.List;

public interface ReadOnlyNonUniqueIndex<K> {

    /**
     * For non-unique hash indexes
     * A bucket may contain many records for the same key
     */
    default IRecordIterator<IKey> iterator(IKey key){
        throw new IllegalStateException("No iterator for a key supported by this index.");
    }

    /**
     * For non-unique indexes
     */
    default List<Object[]> records(K key){
        throw new IllegalStateException("No support for multiple key lookup in this index.");
    }

}
