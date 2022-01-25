package dk.ku.di.dms.vms.infra;

import dk.ku.di.dms.vms.database.query.parse.Statement;

import java.io.Serializable;
import java.util.List;

public interface IRepository<PK extends Serializable,T extends AbstractEntity> {

    public T insert(T object);
    public T insertAll(List<T> objects);

    public T update(T object);
    public T updateAll(List<T> objects);

    public T delete(T object);
    public T deleteAll(List<T> objects);
    public T deleteByKey(PK key);
    public T deleteAllByKey(List<PK> keys);

    public List<T> query(Qualification qualification);
    public T lookupByKey(PK key);

    public Object fetch(Statement sql);

}
