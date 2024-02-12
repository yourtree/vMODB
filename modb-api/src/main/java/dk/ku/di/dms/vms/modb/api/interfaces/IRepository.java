package dk.ku.di.dms.vms.modb.api.interfaces;

import dk.ku.di.dms.vms.modb.api.query.statement.IStatement;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * User-facing database operations
 * @param <PK> The primary key of the entity
 * @param <T> The entity type
 */
public interface IRepository<PK extends Serializable, T extends IEntity<PK>> {

    void insert(T object);
    T insertAndGet(T object);
    void insertAll(List<T> entities);
    List<T> insertAllAndGet(List<T> entities);

    void update(T object);
    void updateAll(List<T> entities);

    void delete(T object);
    void deleteAll(List<T> entities);
    void deleteByKey(PK key);
    void deleteAllByKey(List<PK> keys);

    T lookupByKey(PK key);
    List<T> lookupByKeys(Collection<PK> keys);

    List<T> getAll();

    List<T> query(SelectStatement selectStatement);

    <DTO> List<DTO> query(SelectStatement selectStatement, Class<DTO> clazz);

    /**
     * Used for issuing update, insert, and delete statements.
     * It does not return any value because
     * any error is handled by the system itself
     * Can be insert, update, delete
      */
    void issue(IStatement statement);

    /**
     *  Used for retrieving a single row.
     *  Can't be tied to {@link Record} because the return can be a primitive type.
     *  Provides more flexibility compared to the conventional CRUD API
     */
    <DTO> DTO fetchOne(SelectStatement statement, Class<DTO> clazz);

    <DTO> IVmsFuture<DTO> fetchOnePromise(SelectStatement statement, Class<DTO> clazz);

    /**
     * Used for retrieving multiple rows.
     * Different method given strong typed system
     * Return type no concept of rows
     */
    <DTO> List<DTO> fetchMany(SelectStatement statement, Class<DTO> clazz);

    // maybe callbacks can be a good alternative for embedding statements inside repository
    // <DTO extends Record> List<DTO>  create(Consumer<UpdateStatementBuilder.SetClause> callback);

//    IVmsFuture<Void> submit(Runnable runnable);

}