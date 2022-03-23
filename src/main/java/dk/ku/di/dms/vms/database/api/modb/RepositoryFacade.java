package dk.ku.di.dms.vms.database.api.modb;

import dk.ku.di.dms.vms.database.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.database.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.database.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.database.query.executor.SequentialQueryExecutor;
import dk.ku.di.dms.vms.database.query.parser.stmt.IStatement;
import dk.ku.di.dms.vms.database.query.planner.operator.result.DataTransferObjectOperatorResult;
import dk.ku.di.dms.vms.database.query.planner.tree.PlanNode;
import dk.ku.di.dms.vms.database.query.planner.Planner;
import dk.ku.di.dms.vms.database.store.table.Table;
import dk.ku.di.dms.vms.infra.AbstractEntity;
import dk.ku.di.dms.vms.infra.IRepository;
import dk.ku.di.dms.vms.metadata.VmsMetadata;
import dk.ku.di.dms.vms.proxy.DynamicInvocationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

public final class RepositoryFacade implements InvocationHandler {

    private static Logger LOGGER = LoggerFactory.getLogger(DynamicInvocationHandler.class);

    private final Class<?> pkClazz;

    private final Class<? extends AbstractEntity<?>> entityClazz;

    // DBMS components
    private VmsMetadata vmsMetadata;
    private Analyzer analyzer;
    private Planner planner;

    @SuppressWarnings({"unchecked"})
    public RepositoryFacade(final Class<? extends IRepository<?,?>> repositoryClazz){

        Type[] types = ((ParameterizedType) repositoryClazz.getGenericInterfaces()[0]).getActualTypeArguments();

        this.entityClazz = (Class<? extends AbstractEntity<?>>) types[1];
        this.pkClazz = (Class<?>) types[0];
    }

    public void setAnalyzer(final Analyzer analyzer){
        this.analyzer = analyzer;
    }

    public void setPlanner(final Planner planner){
        this.planner = planner;
    }

    public void setVmsMetadata(VmsMetadata vmsMetadata) {
        this.vmsMetadata = vmsMetadata;
    }

    /**
     * The actual facade for database operations called by the application-level code.
     * @param proxy
     * @param method
     * @param args
     * @return A DTO (i.e., any class where attribute values are final), a row {@link dk.ku.di.dms.vms.database.store.row.Row}, or set of rows
     * @throws AnalyzerException
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws AnalyzerException {
        LOGGER.info("Invoked method: {}", method.getName());

        String methodName = method.getName();

        switch(methodName){

            case "insert": {
                // TODO receive a plan tree from the planner
                break;
            }
            case "insertAll": {

                // acts as a single transaction, so all constraints, of every single row must be present

                Table tableForInsertion = this.vmsMetadata.getTableByEntityClazz( entityClazz );

                PlanNode node = planner.planBulkInsert(tableForInsertion, (List<? extends AbstractEntity<?>>) args[0]); //(args[0]);

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

