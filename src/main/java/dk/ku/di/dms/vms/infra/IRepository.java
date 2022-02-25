package dk.ku.di.dms.vms.infra;

import dk.ku.di.dms.vms.database.query.parser.stmt.IStatement;

import java.io.Serializable;
import java.util.List;

public interface IRepository<PK extends Serializable,T extends AbstractEntity<PK>> {

    T insert(T object);
    T insertAll(List<T> objects);

    T update(T object);
    T updateAll(List<T> objects);

    T delete(T object);
    T deleteAll(List<T> objects);
    T deleteByKey(PK key);
    T deleteAllByKey(List<PK> keys);

    List<T> query(Qualification qualification);
    T lookupByKey(PK key);

    /**
     * Used for issuing update statements and retrieving a single row (object)
      */
    <DTO> DTO fetch( //List<DTO> c,
                      IStatement statement);

    <DTO> List<DTO> fetchList( //List<DTO> c,
                     IStatement statement);

}
