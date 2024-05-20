package dk.ku.di.dms.vms.sdk.embed.facade;

import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
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
 * Responsible to parse objects and convert database results to application objects
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

    /*
     * Cache of objects in memory.
     * Circular buffer of records (represented as object arrays) for a given index
     * Should be used by the repository facade, since it is the one who is converting the payloads from the user code.
     * Key is the hash code of a table
     */
    // private final CircularBuffer objectCacheStore;

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
        for(int i = 0; i < args.length; i++) {
            selectStatement.whereClause.get(i).setValue( args[i] );
        }

        // submit for execution
        List<Object[]> records = this.operationalAPI.fetch( this.table, selectStatement );

        List<T> result = new ArrayList<>(records.size());
        for(Object[] record : records) {
            T ent = this.parseObjectIntoEntity(record);
            if(ent != null) result.add( ent );
        }

        return result;
    }

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

        // parse object into entity
        return this.parseObjectIntoEntity(object);
    }

    @Override
    public final List<T> lookupByKeys(Collection<PK> keys){
        List<T> resultList = new ArrayList<>(keys.size());
        for(PK obj : keys){
            Object[] valuesOfKey = this.extractFieldValuesFromKeyObject(obj);
            Object[] object = this.operationalAPI.lookupByKey(this.table.primaryKeyIndex(), valuesOfKey);
            T ent = this.parseObjectIntoEntity(object);
            if(ent != null)
                resultList.add(ent);
        }
        return resultList;
    }

    @Override
    public final void insert(T entity) {
        Object[] values = this.extractFieldValuesFromEntityObject(entity);
        this.operationalAPI.insert(this.table, values);
    }

    @Override
    public final T insertAndGet(T entity){
        // TODO check if PK is generated before returning the entity
        Object[] values = this.extractFieldValuesFromEntityObject(entity);
        // Object valuesWithKey =
        Object[] newValues = this.operationalAPI.insertAndGet(this.table, values);
        // this.setKeyValueOnObject( key_, cached );
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
        for (T entityObject : entities){
            Object[] parsed = this.extractFieldValuesFromEntityObject(entityObject);
            parsedEntities.add(parsed);
        }
        this.operationalAPI.deleteAll( this.table, parsedEntities );
    }

    public final T parseObjectIntoEntity(Object[] object){
        // all entities must have default constructor
        if (object == null)
            return null;
        try {
            T entity = this.entityConstructor.newInstance();
            int i;
            for(var entry : this.entityFieldMap.entrySet()){
                // must get the index of the column first
                i = this.table.underlyingPrimaryKeyIndex().schema().columnPosition(entry.getKey());
                if(object[i] == null){
                    continue;
                }
                entry.getValue().set( entity, object[i] );
            }
            return entity;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
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

    public final Object[] extractFieldValuesFromEntityObject(T entityObject) {
        Object[] values = new Object[this.table.schema().columnNames().length];
        // TODO objectCacheStore.peek()

        int fieldIdx = 0;
        // get values from entity
        for(String columnName : this.table.schema().columnNames()){
            values[fieldIdx] = this.entityFieldMap.get(columnName).get(entityObject);
            fieldIdx++;
        }
        return values;
    }

    /**
     * TODO This implementation should be used is a possible implementation with a sidecar
     * We can return objects in embedded mode. See last method
     */
//    @Override
//    @SuppressWarnings("unchecked")
//    public <DTO> List<DTO> fetchMany(SelectStatement statement, Class<DTO> clazz){
//        // we need some context about the results in this memory space
//        // solved by now with the assumption the result is of the same size of memory
//        // number of records
//        // schema of the return (maybe not if it is a dto)
//        var memRes = this.operationalAPI.fetchMemoryReference(this.table, statement);
//        try {
//            List<DTO> result = new ArrayList<>(10);
//            Constructor<?> constructor = clazz.getDeclaredConstructors()[0];
//            Field[] fields = clazz.getFields();
//            long currAddress = memRes.address();
//            long lastOffset = currAddress + memRes.bytes();
//            while(memRes != null) {
//                DTO dto = (DTO) constructor.newInstance();
//                for (var field : fields) {
//                    DataType dt = DataTypeUtils.getDataTypeFromJavaType(field.getType());
//                    field.set(dto, DataTypeUtils.callReadFunction(currAddress, dt));
//                    currAddress += dt.value;
//                }
//                result.add(dto);
//
//                if(currAddress == lastOffset) {
//                    MemoryManager.releaseTemporaryDirectMemory(memRes.address());
//                    memRes = memRes.next;
//                    if(memRes != null){
//                        memRes = memRes.next;
//                        currAddress = memRes.address();
//                        lastOffset = currAddress + memRes.bytes();
//                    }
//                }
//            }
//            return result;
//        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
//            throw new RuntimeException(e);
//        }
//    }

    @Override
    @SuppressWarnings("unchecked")
    public <DTO> List<DTO> query(SelectStatement statement, Class<DTO> clazz){
        List<Object[]> objects = this.operationalAPI.fetch(this.table, statement);
        Constructor<?> constructor = clazz.getDeclaredConstructors()[0];
        Field[] fields = clazz.getFields();
        List<DTO> result = new ArrayList<>();
        try {
            for(Object[] object : objects){
                DTO dto = (DTO) constructor.newInstance();
                int i = 0;
                for (var field : fields) {
                    field.set(dto, object[i]);
                    i++;
                }
                result.add(dto);
            }
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

}

