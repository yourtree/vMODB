package dk.ku.di.dms.vms.modb.index.interfaces;

import dk.ku.di.dms.vms.modb.definition.key.IKey;

public interface ReadWriteIndex<K> extends IIndex<K> {

    void insert(IKey key, Object[] record);

    void update(IKey key, Object[] record);

    void delete(IKey key);

    Object[] lookupByKey(IKey key);

}
