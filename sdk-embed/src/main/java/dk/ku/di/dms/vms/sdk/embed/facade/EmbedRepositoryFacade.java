package dk.ku.di.dms.vms.sdk.embed.facade;

import dk.ku.di.dms.vms.modb.api.interfaces.IDTO;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.modb.api.query.statement.IStatement;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Row;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.modb.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractOperator;
import dk.ku.di.dms.vms.modb.transaction.TransactionFacade;
import dk.ku.di.dms.vms.modb.transaction.internal.CircularBuffer;
import dk.ku.di.dms.vms.modb.transaction.multiversion.ConsistentIndex;
import dk.ku.di.dms.vms.modb.transaction.multiversion.TransactionWrite;
import dk.ku.di.dms.vms.sdk.core.facade.IVmsRepositoryFacade;
import dk.ku.di.dms.vms.sdk.embed.entity.EntityUtils;

import java.io.Serializable;
import java.lang.invoke.VarHandle;
import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.logging.Logger;

/**
 * The embed repository facade contains references to DBMS components
 * since the application is co-located with the MODB
 */
public final class EmbedRepositoryFacade implements IVmsRepositoryFacade, InvocationHandler {

    private static final Logger LOGGER = Logger.getLogger(EmbedRepositoryFacade.class.getName());

    private final Class<? extends IEntity<?>> entityClazz;
    private final Class<? extends Serializable> pkClazz;

    private final Constructor<? extends IEntity<?>> entityConstructor;

    private Table table;

    /**
     * Respective consistent index
     */
    private ConsistentIndex consistentIndex;

    // DBMS components
    private ModbModules modbModules;

    private final Map<String, AbstractOperator> cachedPlans;

    private Map<String, VarHandle> entityFieldMap;

    private Map<String, VarHandle> pkFieldMap;

    /**
     * Cache of objects in memory.
     * Circular buffer of records (represented as object arrays) for a given index
     * Should be used by the repository facade, since it is the one who is converting the payloads from the user code.
     * Key is the hash code of a table
     */
    private CircularBuffer objectCacheStore;

    private final ConcurrentLinkedDeque<Map<IKey, TransactionWrite>> tidWriteMapCacheStore;

    @SuppressWarnings({"unchecked"})
    public EmbedRepositoryFacade(final Class<? extends IRepository<?,?>> repositoryClazz) throws NoSuchMethodException {

        Type[] types = ((ParameterizedType) repositoryClazz.getGenericInterfaces()[0]).getActualTypeArguments();

        this.entityClazz = (Class<? extends IEntity<?>>) types[1];

        this.pkClazz = (Class<? extends Serializable>) types[0];

        // read-only transactions may put items here
        this.cachedPlans = new ConcurrentHashMap<>();

        this.entityConstructor = this.entityClazz.getDeclaredConstructor();

        this.tidWriteMapCacheStore = new ConcurrentLinkedDeque<>();

    }

    public void setModbModules(ModbModules modbModules) throws NoSuchFieldException, IllegalAccessException {

        this.modbModules = modbModules;

        String tableName = modbModules.vmsRuntimeMetadata().entityToTableNameMap().get( this.entityClazz );

        Schema schema = modbModules.catalog().getTable(tableName).getSchema();

        // https://stackoverflow.com/questions/43558270/correct-way-to-use-varhandle-in-java-9
        this.entityFieldMap = EntityUtils.getFieldsFromEntity( this.entityClazz, schema );

        if(!pkClazz.isPrimitive()){
            this.pkFieldMap = EntityUtils.getFieldsFromPk( this.pkClazz );
        }

        this.table = this.modbModules.catalog().getTable( tableName );

        this.objectCacheStore = new CircularBuffer(table.getSchema().columnOffset().length);

        // TODO set up consistent index
        this.consistentIndex = null;

    }

    /**
     * The actual facade for database operations called by the application-level code.
     * @param proxy the virtual microservice caller method (a subtransaction)
     * @param method the repository method called
     * @param args the function call parameters
     * @return A DTO (i.e., any class where attribute values are final), a row {@link Row}, or set of rows
     */
    @Override
    @SuppressWarnings("unchecked")
    public Object invoke(Object proxy, Method method, Object[] args) {

        String methodName = method.getName();

        switch (methodName) {
            case "lookupByKey" -> {

                // this repository is oblivious to multi-versioning
                // given the single-thread model, we can work with writes easily
                // but reads are another story. multiple threads may be calling the repository facade
                // and requiring different data item versions
                // we always need to offload the lookup to the transaction facade
                Object[] valuesOfKey = this.extractFieldValuesFromKeyObject(this.pkFieldMap, args[0]);
                Object[] object = TransactionFacade.lookupByKey(consistentIndex, valuesOfKey);

                // parse object into entity
                if (object != null)
                    return parseObjectIntoEntity(object);
                return null;

            }
            case "deleteByKey" -> {
                Object[] valuesOfKey = this.extractFieldValuesFromKeyObject(this.pkFieldMap, args[0]);
                TransactionFacade.deleteByKey(this.consistentIndex, valuesOfKey);
            }
            case "delete" -> {
                Object[] values = this.extractFieldValuesFromEntityObject(this.entityFieldMap, args[0], this.table);
                TransactionFacade.delete(this.consistentIndex, values);
            }
            case "update" -> {
                Object[] values = extractFieldValuesFromEntityObject(this.entityFieldMap, args[0], this.table);
                TransactionFacade.update(this.consistentIndex, values);
            }
            case "insert" -> {
                String tableName = this.modbModules.vmsRuntimeMetadata().entityToTableNameMap().get(entityClazz);
                Table table = this.modbModules.catalog().getTable(tableName);
                Object[] values = extractFieldValuesFromEntityObject(this.entityFieldMap, args[0], table);
                TransactionFacade.insert(this.consistentIndex, values);
            }
            case "insertAll" -> this.insertAll((List<Object>) args[0]);
            case "fetch" -> {
                // dispatch to analyzer passing the clazz param
                // always select because of the repository API
                SelectStatement selectStatement = ((IStatement) args[0]).asSelectStatement();
                return fetch(selectStatement, (Type) args[1]);
            }
            case "issue" -> issue((IStatement) args[0]);
            default -> {

                // check if is it static query
                SelectStatement selectStatement = modbModules.vmsRuntimeMetadata().staticQueries().get(methodName);

                if (selectStatement == null)
                    throw new IllegalStateException("Unknown repository operation.");

                return fetch(selectStatement);

            }
        }

        return null;
    }

    private IEntity<?> parseObjectIntoEntity( Object[] object ){
        // all entities must have default constructor
        try {
            IEntity<?> entity = entityConstructor.newInstance();
            int i = 0;
            for(var entry : entityFieldMap.entrySet()){
                entry.getValue().set( entity, object[i] );
                i++;
            }
            return entity;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Must be a linked sorted map. Ordered by the columns that appear on the key object.
     */
    private Object[] extractFieldValuesFromKeyObject(Map<String, VarHandle> fieldMap, Object keyObject) {

        Object[] values = new Object[fieldMap.size()];

        int fieldIdx = 0;
        // get values from key object
        for(String columnName : fieldMap.keySet()){
            values[fieldIdx] = fieldMap.get(columnName).get(keyObject);
            fieldIdx++;
        }
        return values;
    }

    private Object[] extractFieldValuesFromEntityObject(Map<String, VarHandle> fieldMap, Object entityObject, Table table) {

        Object[] values = new Object[table.getSchema().getColumnNames().length];
        // TODO objectCacheStore.peek()

        int fieldIdx = 0;
        // get values from entity
        for(String columnName : table.getSchema().getColumnNames()){
            values[fieldIdx] = fieldMap.get(columnName).get(entityObject);
            fieldIdx++;
        }
        return values;
    }

    // TODO FINISH
//    private Object[] getObjectFromCacheStore(Table table){
//        this.objectCacheStore.get(table) != null
//    }

    /**
     * Best guess return type. Differently from the parameter type received.
     * @param selectStatement a select statement
     * @return the query result
     */
    public Object fetch(SelectStatement selectStatement) {
        return fetch(selectStatement,null);
    }

    @Override
    public Object fetch(SelectStatement selectStatement, Type type) {

        String sqlAsKey = selectStatement.SQL.toString();

        AbstractOperator scanOperator = cachedPlans.get( sqlAsKey );

        List<WherePredicate> wherePredicates;

        if(scanOperator == null){
            QueryTree queryTree;
            try {
                queryTree = modbModules.analyzer().analyze(selectStatement);
                wherePredicates = queryTree.wherePredicates;
                scanOperator = modbModules.planner().plan(queryTree);
                cachedPlans.put(sqlAsKey, scanOperator );
            } catch (AnalyzerException ignored) { return null; }

        } else {
            // get only the where clause params
            try {
                wherePredicates = modbModules.analyzer().analyzeWhere(
                        scanOperator.asScan().table, selectStatement.whereClause);
            } catch (AnalyzerException ignored) { return null; }
        }

        MemoryRefNode memRes = null;

        // TODO complete for all types or migrate the choice to transaction facade
        //  make an enum, it is easier
        if(scanOperator.isIndexScan()){
            // build keys and filters
            //memRes = OperatorExecution.run( wherePredicates, scanOperator.asIndexScan() );
            memRes = TransactionFacade.run(this.consistentIndex, wherePredicates, scanOperator.asIndexScan());
        } else {
            // build only filters
            memRes = TransactionFacade.run( this.consistentIndex, wherePredicates, scanOperator.asFullScan() );
        }

        // TODO parse output into object
        if(type == IDTO.class) {
            // look in the map of dto types for the setter and getter
            return null;
        }

        // then it is a primitive, just return the value
        int projectionColumnIndex = scanOperator.asScan().projectionColumns[0];
        DataType dataType = scanOperator.asScan().index.schema().getColumnDataType(projectionColumnIndex);
        return DataTypeUtils.getValue(dataType, memRes.address);

    }

    /**
     * TODO finish. can we extract the column values and make a special api for the facade?
     * @param statement
     */
    private void issue(IStatement statement) {

        switch (statement.getType()){
            case UPDATE -> {

            }
            case INSERT -> {

            }
            case DELETE -> {

            }
            default -> throw new IllegalStateException("Statement type cannot be identified.");
        }

    }

    @Override
    public void insertAll(List<Object> entities) {

        // acts as a single transaction, so all constraints, of every single row must be present
        List<Object[]> parsedEntities = new ArrayList<>(entities.size());
        for (Object entityObject : entities){
            Object[] parsed = extractFieldValuesFromEntityObject(this.entityFieldMap, entityObject, table);
            parsedEntities.add(parsed);
        }

        // can only add to cache if all items were inserted since it is transactional
        TransactionFacade.insertAll( this.consistentIndex, parsedEntities );

    }

    @Override
    public InvocationHandler asInvocationHandler() {
        return this;
    }
}

