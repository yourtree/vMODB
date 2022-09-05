package dk.ku.di.dms.vms.modb.common.data_structure;

/**
 * Basic entry to represent pair of values
 * @param <K> key
 * @param <V> value
 */
public class KeyValueEntry<K,V> {

    private K key;
    private V value;

    public KeyValueEntry(K key, V value){
        this.key = key;
        this.value = value;
    }

    public K getKey(){
        return key;
    }

    public V getValue(){
        return value;
    }

}
