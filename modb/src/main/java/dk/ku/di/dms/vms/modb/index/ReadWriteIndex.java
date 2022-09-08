package dk.ku.di.dms.vms.modb.index;

public interface ReadWriteIndex<K> extends ReadOnlyIndex<K> {

    void insert(K key, long srcAddress);

    void update(K key, long srcAddress);

    void delete(K key);

}
