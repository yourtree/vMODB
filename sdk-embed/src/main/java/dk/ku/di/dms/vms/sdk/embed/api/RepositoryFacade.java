package dk.ku.di.dms.vms.sdk.embed.api;

import dk.ku.di.dms.vms.modb.common.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.common.interfaces.IRepository;
import dk.ku.di.dms.vms.modb.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.modb.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.modb.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.modb.query.executor.SequentialQueryExecutor;
import dk.ku.di.dms.vms.modb.common.query.statement.IStatement;
import dk.ku.di.dms.vms.sdk.embed.VmsMetadataEmbed;
import dk.ku.di.dms.vms.modb.query.planner.Planner;
import dk.ku.di.dms.vms.modb.query.planner.operator.result.DataTransferObjectOperatorResult;
import dk.ku.di.dms.vms.modb.query.planner.tree.PlanNode;
import dk.ku.di.dms.vms.modb.common.meta.Row;
import dk.ku.di.dms.vms.modb.store.table.Table;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.logging.Logger;

public final class RepositoryFacade implements InvocationHandler {

    private static Logger LOGGER = Logger.getLogger(RepositoryFacade.class.getName());

    private final Class<?> pkClazz;

    private final Class<? extends IEntity<?>> entityClazz;

    // DBMS components
    private VmsMetadataEmbed vmsMetadata;
    private Analyzer analyzer;
    private Planner planner;

    @SuppressWarnings({"unchecked"})
    public RepositoryFacade(final Class<? extends IRepository<?,?>> repositoryClazz){

        Type[] types = ((ParameterizedType) repositoryClazz.getGenericInterfaces()[0]).getActualTypeArguments();

        this.entityClazz = (Class<? extends IEntity<?>>) types[1];
        this.pkClazz = (Class<?>) types[0];
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

                PlanNode node = planner.planBulkInsert(tableForInsertion, (List<? extends IEntity<?>>) args[0]); //(args[0]);

                SequentialQueryExecutor queryExecutor = new SequentialQueryExecutor(node);

                queryExecutor.get();

                break;
            }
            case "fetch": {

                // dispatch to analyzer passing the clazz param
                QueryTree queryTree = analyzer.analyze( (IStatement) args[0], (Class<?>) args[1]);
                PlanNode node = planner.plan( queryTree );

                // TODO get concrete executor from application metadata
                SequentialQueryExecutor queryExecutor = new SequentialQueryExecutor(node);

                DataTransferObjectOperatorResult result = queryExecutor.get().asDataTransferObjectOperatorResult();
                return result.getDataTransferObjects().size() > 1 ?
                        result.getDataTransferObjects() : result.getDataTransferObjects().get(0);

            }
            default: throw new IllegalStateException("Unknown repository operation.");
        }

        return null;
    }
}

