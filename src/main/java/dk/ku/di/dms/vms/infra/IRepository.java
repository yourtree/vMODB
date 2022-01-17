package dk.ku.di.dms.vms.infra;

import java.util.List;

public interface IRepository<K,T> {

    public T insert(T object);
    public T insertAll(List<T> objects);

    public T update(T object);
    public T updateAll(List<T> objects);

    public T delete(T object);
    public T deleteAll(List<T> objects);
    public T deleteByKey(K key);
    public T deleteAllByKey(List<K> keys);

    public List<T> query(Qualification qualification);
    public T lookupByKey(K key);

    public Object fetch(String sql);

}
