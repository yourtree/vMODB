package dk.ku.di.dms.vms.modb.index.interfaces;

public interface ReadWriteIndex<K> extends ReadOnlyIndex<K> {

    void insert(K key, long srcAddress);

    void update(K key, long srcAddress);

    void insert(K key, Object[] record);

    void update(K key, Object[] record);

    void delete(K key);

}
