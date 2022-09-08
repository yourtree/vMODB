package dk.ku.di.dms.vms.sdk.embed.facade;

import dk.ku.di.dms.vms.modb.api.interfaces.IDTO;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.modb.api.query.statement.IStatement;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.definition.Row;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.modb.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.modb.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractOperator;
import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.storage.memory.DataTypeUtils;
import dk.ku.di.dms.vms.modb.transaction.TransactionFacade;
import dk.ku.di.dms.vms.sdk.core.facade.IVmsRepositoryFacade;
import dk.ku.di.dms.vms.sdk.embed.VmsMetadataEmbed;
import dk.ku.di.dms.vms.modb.query.planner.Planner;

import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * The embed repository facade contains references to DBMS components
 * since the application is co-located with the MODB
 */
public final class EmbedRepositoryFacade implements IVmsRepositoryFacade, InvocationHandler {

    private static Logger LOGGER = Logger.getLogger(EmbedRepositoryFacade.class.getName());

    private final Class<?> pkClazz;

    private final Class<? extends IEntity<?>> entityClazz;

    // DBMS components
    private VmsMetadataEmbed vmsMetadata;
    private Analyzer analyzer;
    private Planner planner;

    private TransactionFacade transactionFacade;

    private Map<String, AbstractOperator> cachedPlans;

    List<Field> entityFields;

    @SuppressWarnings({"unchecked"})
    public EmbedRepositoryFacade(final Class<? extends IRepository<?,?>> repositoryClazz){

        Type[] types = ((ParameterizedType) repositoryClazz.getGenericInterfaces()[0]).getActualTypeArguments();

        this.entityClazz = (Class<? extends IEntity<?>>) types[1];
        this.pkClazz = (Class<?>) types[0];

        // read-only transactions may put items here
        this.cachedPlans = new ConcurrentHashMap<>();
    }

    public void setAnalyzer(final Analyzer analyzer){
        this.analyzer = analyzer;
    }

    public void setPlanner(final Planner planner){
        this.planner = planner;
    }

    public void setVmsMetadata(VmsMetadataEmbed vmsMetadata) {
        this.vmsMetadata = vmsMetadata;
    }

    /**
     * The actual facade for database operations called by the application-level code.
     * @param proxy
     * @param method
     * @param args
     * @return A DTO (i.e., any class where attribute values are final), a row {@link Row}, or set of rows
     * @throws AnalyzerException
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws IllegalAccessException {

        String methodName = method.getName();

        switch(methodName){

            // intermediate buffer. heap concurrent hash map
            // hash is table + record id hashed
            // values are versions. in the absence of versions, read from store
            // an object will inform the oldest and newest TIDs and contain reference to the versions

            case "insert": {

                Table table = this.vmsMetadata.getTableByEntityClazz( entityClazz );

                int recordSize = table.getSchema().getRecordSize();

                ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer(recordSize);

                // get values from entity

                // Row row
                Object[] values = new Object[table.getSchema().getColumnNames().length];

                int fieldIdx = 0;
                for(String columnName : table.getSchema().getColumnNames()){
                    values[fieldIdx] = entityFields.get(fieldIdx).get( args[0] );
                }

                break;
            }
            case "insertAll": {

                // acts as a single transaction, so all constraints, of every single row must be present

                Table tableForInsertion = this.vmsMetadata.getTableByEntityClazz( entityClazz );

//                PlanNode node = planner.planBulkInsert(tableForInsertion, (List<? extends IEntity<?>>) args[0]); //(args[0]);
//
//                SequentialQueryExecutor queryExecutor = new SequentialQueryExecutor(node);
//
//                queryExecutor.get();

                break;
            }
            case "fetch": {
                // dispatch to analyzer passing the clazz param
                // always select because of the repository API
                SelectStatement selectStatement = ((IStatement) args[0]).asSelectStatement();
                return fetch(selectStatement, (Type)args[1]);
            }
            default: throw new IllegalStateException("Unknown repository operation.");
        }

        return null;
    }

    @Override
    public Object fetch(SelectStatement selectStatement, Type type) {

        String sqlAsKey = selectStatement.SQL.toString();

        AbstractOperator scanOperator = cachedPlans.get( sqlAsKey );

        List<WherePredicate> wherePredicates;

        if(scanOperator == null){
            QueryTree queryTree;
            try {
                queryTree = analyzer.analyze(selectStatement);
                wherePredicates = queryTree.wherePredicates;
                scanOperator = planner.plan(queryTree);
                cachedPlans.put(sqlAsKey, scanOperator );
            } catch (AnalyzerException ignored) { return null; }

        } else {
            // get only the where clause params
            try {
                wherePredicates = analyzer.analyzeWhere(
                        scanOperator.asScan().table, selectStatement.whereClause);
            } catch (AnalyzerException ignored) { return null; }
        }

        MemoryRefNode memRes = null;
        // MemoryRefNode memRes = transactionFacade.;
        if(scanOperator.isIndexScan()){
            // build keys and filters
            //memRes = OperatorExecution.run( wherePredicates, scanOperator.asIndexScan() );
        } else {
            // build only filters
            //memRes = OperatorExecution.run( wherePredicates, scanOperator.asFullScan() );
        }

        // parse output into object
        if( type == IDTO.class) {
            // look in the map of dto types for the setter and getter
            return null;
        }

        // then it is a primitive, just return the value
        int projectionColumnIndex = scanOperator.asScan().projectionColumns[0];
        DataType dataType = scanOperator.asScan().index.getTable().getSchema().getColumnDataType(projectionColumnIndex);
        return DataTypeUtils.getValue(dataType, memRes.address);

    }
}

