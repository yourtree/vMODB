package dk.ku.di.dms.vms.sdk.embed.api;

import dk.ku.di.dms.vms.modb.common.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.common.interfaces.IRepository;
import dk.ku.di.dms.vms.modb.common.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.definition.Row;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.modb.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.modb.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.modb.common.query.statement.IStatement;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.query.planner.execution.OperatorExecution;
import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractOperator;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryRefNode;
import dk.ku.di.dms.vms.sdk.core.client.IVmsRepositoryFacade;
import dk.ku.di.dms.vms.sdk.embed.VmsMetadataEmbed;
import dk.ku.di.dms.vms.modb.query.planner.Planner;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public final class RepositoryFacade implements IVmsRepositoryFacade, InvocationHandler {

    private static Logger LOGGER = Logger.getLogger(RepositoryFacade.class.getName());

    private final Class<?> pkClazz;

    private final Class<? extends IEntity<?>> entityClazz;

    // DBMS components
    private VmsMetadataEmbed vmsMetadata;
    private Analyzer analyzer;
    private Planner planner;

    private Map<String, AbstractOperator> cachedPlans;

    @SuppressWarnings({"unchecked"})
    public RepositoryFacade(final Class<? extends IRepository<?,?>> repositoryClazz){

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
    public Object invoke(Object proxy, Method method, Object[] args) throws AnalyzerException {

        String methodName = method.getName();

        switch(methodName){

            case "insert": {
                // TODO receive a plan tree from the planner




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
                    wherePredicates = analyzer.analyzeWhere(
                            scanOperator.asScan().index.getTable(), selectStatement.whereClause);
                }


                if(scanOperator.isIndexScan()){
                    // build keys and filters
                    MemoryRefNode memRes = OperatorExecution.run( wherePredicates, scanOperator.asIndexScan() );
                } else {
                    // build only filters

                }



            }
            default: throw new IllegalStateException("Unknown repository operation.");
        }

        return null;
    }


}

