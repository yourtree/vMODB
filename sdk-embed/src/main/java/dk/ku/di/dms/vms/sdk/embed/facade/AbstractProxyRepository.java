package dk.ku.di.dms.vms.sdk.embed.facade;

import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.modb.api.query.clause.WhereClauseElement;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.transaction.OperationalAPI;
import dk.ku.di.dms.vms.sdk.embed.entity.EntityUtils;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.This;

import java.io.Serializable;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The interface between the application and internal database operations
 * Responsible to parse objects and convert database results to application objects.
 * A repository class do not need direct access to {@link ITransactionManager}
 */
public abstract class AbstractProxyRepository<PK extends Serializable, T extends IEntity<PK>> implements IRepository<PK, T> {

//    private static final Logger LOGGER = Logger.getLogger(AbstractProxyRepository.class.getName());

    private final Constructor<T> entityConstructor;

    /**
     * Respective table of the entity
     */
    private final Table table;

    private final Map<String, VarHandle> entityFieldMap;

    private final Map<String, VarHandle> pkFieldMap;

    /**
     * Attribute set after database is loaded
     * Not when the metadata is loaded
     * The transaction facade requires DBMS modules (planner, analyzer)
     * along with the catalog. These are not ready on metadata loading time
     */
    private final OperationalAPI operationalAPI;

    private final Map<String, SelectStatement> repositoryQueriesMap;

    public AbstractProxyRepository(Class<PK> pkClazz,
                                  Class<T> entityClazz,
                                  Table table,
                                  OperationalAPI operationalAPI,
                                  // key: method name, value: select stmt
                                  Map<String, SelectStatement> repositoryQueriesMap)
            throws NoSuchFieldException, IllegalAccessException {
        this.entityConstructor = EntityUtils.getEntityConstructor(entityClazz);
        this.entityFieldMap = EntityUtils.getVarHandleFieldsFromEntity(entityClazz, table.schema());
        if(pkClazz.getPackageName().equalsIgnoreCase("java.lang") || pkClazz.isPrimitive()){
            this.pkFieldMap = EntityUtils.getVarHandleFieldFromPk(entityClazz, table.schema());
        } else {
            this.pkFieldMap = EntityUtils.getVarHandleFieldsFromCompositePk(pkClazz);
        }
        this.table = table;
        this.operationalAPI = operationalAPI;
        this.repositoryQueriesMap = repositoryQueriesMap;
    }

    /**
     * <a href="http://mydailyjava.blogspot.com/2022/02/using-byte-buddy-for-proxy-creation.html">Proxy creation in ByteBuddy</a>
     */
    @SuppressWarnings("rawtypes")
    public static final class Interceptor {
        @RuntimeType
        public static Object intercept(
                @This AbstractProxyRepository self,
                @Origin Method method,
                @AllArguments Object[] args) {
            return self.intercept(method.getName(), args);
        }
    }

    public final List<T> intercept(String methodName, Object[] args) {
        // retrieve statically defined query
        SelectStatement selectStatement = this.repositoryQueriesMap.get(methodName);

        // set query arguments
        List<WhereClauseElement> whereClauseElements = new ArrayList<>(args.length);
        for(int i = 0; i < args.length; i++) {
            whereClauseElements.add(selectStatement.whereClause.get(i).overwriteValue( args[i] ));
        }
        selectStatement = selectStatement.clone(whereClauseElements);

        // submit for execution
        List<Object[]> records = this.operationalAPI.fetch(this.table, selectStatement);
        List<T> result = new ArrayList<>(records.size());
        for(Object[] record : records) {
            result.add( this.parseObjectIntoEntity(record) );
        }
        return result;
    }

    @Override
    public final List<T> getAll(){
        List<Object[]> records = this.operationalAPI.getAll(this.table);
        List<T> resultList = new ArrayList<>(records.size());
        for (var record : records){
            resultList.add(this.parseObjectIntoEntity(record));
        }
        return resultList;
    }

    @Override
    public final boolean exists(PK key){
        Object[] valuesOfKey = this.extractFieldValuesFromKeyObject(key);
        return this.operationalAPI.exists(this.table.primaryKeyIndex(), valuesOfKey);
    }

    @Override
    public final T lookupByKey(PK key){
        // this repository is oblivious to multi-versioning
        // given the single-thread model, we can work with writes easily
        // but reads are another story. multiple threads may be calling the repository facade
        // and requiring different data item versions
        // we always need to offload the lookup to the transaction facade
        Object[] valuesOfKey = this.extractFieldValuesFromKeyObject(key);
        Object[] object = this.operationalAPI.lookupByKey(this.table.primaryKeyIndex(), valuesOfKey);
        if (object == null) return null;
        // parse object into entity
        return this.parseObjectIntoEntity(object);
    }

    @Override
    public final List<T> lookupByKeys(Collection<PK> keys){
        List<T> resultList = new ArrayList<>(keys.size());
        for(PK obj : keys){
            Object[] valuesOfKey = this.extractFieldValuesFromKeyObject(obj);
            Object[] object = this.operationalAPI.lookupByKey(this.table.primaryKeyIndex(), valuesOfKey);
            resultList.add( this.parseObjectIntoEntity(object) );
        }
        return resultList;
    }

    @Override
    public final void insert(T entity) {
        Object[] values = this.extractFieldValuesFromEntityObject(entity);
        this.operationalAPI.insert(this.table, values);
    }

    /**
     * An optimization is just setting the PK into the entity passed as parameter.
     * Another optimization is checking if PK is generated before returning the entity,
     * but this is missing for now. However, this method is probably used when the PK
     * must be retrieved
     */
    @Override
    public final T insertAndGet(T entity){
        Object[] values = this.extractFieldValuesFromEntityObject(entity);
        Object[] newValues = this.operationalAPI.insertAndGet(this.table, values);
        for(var entry : this.pkFieldMap.entrySet()){
            int i = this.table.schema().columnPosition(entry.getKey());
            if(newValues[i] == null){
                continue;
            }
            entry.getValue().set( entity, newValues[i] );
        }
        return entity;
    }

    @Override
    public final void insertAll(List<T> entities) {
        // acts as a single transaction, so all constraints, of every single row must be present
        List<Object[]> parsedEntities = new ArrayList<>(entities.size());
        for (T entityObject : entities){
            Object[] parsed = this.extractFieldValuesFromEntityObject(entityObject);
            parsedEntities.add(parsed);
        }
        // can only add to cache if all items were inserted since it is transactional
        this.operationalAPI.insertAll( this.table, parsedEntities );
    }

    @Override
    public final void upsert(T entity) {
        Object[] values = this.extractFieldValuesFromEntityObject(entity);
        this.operationalAPI.upsert(this.table, values);
    }

    @Override
    public final void update(T entity) {
        Object[] values = this.extractFieldValuesFromEntityObject(entity);
        this.operationalAPI.update(this.table, values);
    }

    @Override
    public final void updateAll(List<T> entities) {
        List<Object[]> parsedEntities = new ArrayList<>(entities.size());
        for (T entityObject : entities){
            Object[] parsed = this.extractFieldValuesFromEntityObject(entityObject);
            parsedEntities.add(parsed);
        }
        this.operationalAPI.updateAll(this.table, parsedEntities);
    }

    @Override
    public final void delete(T entity){
        Object[] values = this.extractFieldValuesFromEntityObject(entity);
        this.operationalAPI.delete(this.table, values);
    }

    @Override
    public final void deleteByKey(PK key) {
        Object[] valuesOfKey = this.extractFieldValuesFromKeyObject(key);
        this.operationalAPI.deleteByKey(this.table, valuesOfKey);
    }

    @Override
    public final void deleteAll(List<T> entities) {
        List<Object[]> parsedEntities = new ArrayList<>(entities.size());
        for (T entity : entities){
            parsedEntities.add( this.extractFieldValuesFromEntityObject(entity) );
        }
        this.operationalAPI.deleteAll(this.table, parsedEntities);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public final T parseObjectIntoEntity(Object[] object){
        try {
            // all entities must have default constructor
            T entity = this.entityConstructor.newInstance();
            int i;
            for(Map.Entry<String,VarHandle> entry : this.entityFieldMap.entrySet()){
                // must get the index of the column first
                i = this.table.schema().columnPosition(entry.getKey());
                if(object[i] == null){
                    continue;
                }
                try {
                    entry.getValue().set(entity, object[i]);
                } catch (ClassCastException e){
                    // has the entry come from raw index?
                    if(this.table.schema().columnDataType(i) == DataType.ENUM && object[i] instanceof String objStr && !objStr.isEmpty() && !objStr.isBlank()){
                        entry.getValue().set(entity, Enum.valueOf((Class)entry.getValue().varType(), objStr));
                    } else {
                        System.out.println("Cannot cast column "+table.schema().columnName(i)+" for value "+object[i]);
                        throw new RuntimeException(e);
                    }
                }
            }
            return entity;
        } catch (ClassCastException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Must be a linked sorted map. Ordered by the columns that appear on the key object.
     */
    private Object[] extractFieldValuesFromKeyObject(PK keyObject) {
        Object[] values = new Object[this.pkFieldMap.size()];
        if(keyObject instanceof Number){
            values[0] = keyObject;
            return values;
        }
        int fieldIdx = 0;
        // get values from key object
        for(String columnName : this.pkFieldMap.keySet()){
            values[fieldIdx] = this.pkFieldMap.get(columnName).get(keyObject);
            fieldIdx++;
        }
        return values;
    }

    public final Object[] extractFieldValuesFromEntityObject(T entity) {
        Object[] values = new Object[this.table.schema().columnNames().length];
        int fieldIdx = 0;
        for(String columnName : this.table.schema().columnNames()){
            values[fieldIdx] = this.entityFieldMap.get(columnName).get(entity);
            fieldIdx++;
        }
        return values;
    }

    /**
     * This implementation should be used is a possible implementation with a sidecar
     * We can return objects in embedded mode. See last method
    public <DTO> List<DTO> fetchMany(SelectStatement statement, Class<DTO> clazz){
        // we need some context about the results in this memory space
        // solved by now with the assumption the result is of the same size of memory
        // number of records
        // schema of the return (maybe not if it is a dto)
        var memRes = this.operationalAPI.fetchMemoryReference(this.table, statement);
        try {
            List<DTO> result = new ArrayList<>(10);
            Constructor<?> constructor = clazz.getDeclaredConstructors()[0];
            Field[] fields = clazz.getFields();
            long currAddress = memRes.address();
            long lastOffset = currAddress + memRes.bytes();
            while(memRes != null) {
                DTO dto = (DTO) constructor.newInstance();
                for (var field : fields) {
                    DataType dt = DataTypeUtils.getDataTypeFromJavaType(field.getType());
                    field.set(dto, DataTypeUtils.callReadFunction(currAddress, dt));
                    currAddress += dt.value;
                }
                result.add(dto);

                if(currAddress == lastOffset) {
                    MemoryManager.releaseTemporaryDirectMemory(memRes.address());
                    memRes = memRes.next;
                    if(memRes != null){
                        memRes = memRes.next;
                        currAddress = memRes.address();
                        lastOffset = currAddress + memRes.bytes();
                    }
                }
            }
            return result;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
     */

    @Override
    public <DTO> DTO fetchOne(SelectStatement statement, Class<DTO> clazz){
        List<Object[]> objects = this.operationalAPI.fetch(this.table, statement);
        assert objects.size() == 1;
        Object[] object = objects.getFirst();
        Constructor<?> constructor = clazz.getDeclaredConstructors()[0];
        Field[] fields = clazz.getFields();
        return this.buildDtoInstance(object, constructor, fields);
    }

    @Override
    public <DTO> List<DTO> fetchMany(SelectStatement statement, Class<DTO> clazz){
        List<Object[]> objects = this.operationalAPI.fetch(this.table, statement);
        Constructor<?> constructor = clazz.getDeclaredConstructors()[0];
        Field[] fields = clazz.getFields();
        List<DTO> result = new ArrayList<>();
        for(Object[] object : objects){
            DTO dto = buildDtoInstance(object, constructor, fields);
            result.add(dto);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private <DTO> DTO buildDtoInstance(Object[] object, Constructor<?> constructor, Field[] fields) {
        try {
            DTO dto = (DTO) constructor.newInstance();
            int i = 0;
            for (var field : fields) {
                // check if field was captured by query
                if(object[i] != null){
                    field.set(dto, object[i]);
                }
                i++;
            }
            return dto;
        } catch (InstantiationException | InvocationTargetException| IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

}

