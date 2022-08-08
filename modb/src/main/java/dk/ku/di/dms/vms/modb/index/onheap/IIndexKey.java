package dk.ku.di.dms.vms.modb.index.onheap;

import dk.ku.di.dms.vms.modb.schema.key.IKey;

/**
 * Serves to identify uniquely an index or set of indexes.
 * Differently from {@link IKey}, which serves for identifying unique values or unique index entries
 */
public interface IIndexKey {

    int hashCode();

}
