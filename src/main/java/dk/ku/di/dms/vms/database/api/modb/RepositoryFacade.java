package dk.ku.di.dms.vms.database.api.modb;

import dk.ku.di.dms.vms.database.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.database.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.database.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.database.query.executor.SequentialQueryExecutor;
import dk.ku.di.dms.vms.database.query.parser.stmt.IStatement;
import dk.ku.di.dms.vms.database.query.planner.operator.OperatorResult;
import dk.ku.di.dms.vms.database.query.planner.tree.PlanNode;
import dk.ku.di.dms.vms.database.query.planner.Planner;
import dk.ku.di.dms.vms.infra.AbstractEntity;
import dk.ku.di.dms.vms.infra.IRepository;
import dk.ku.di.dms.vms.proxy.DynamicInvocationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

public final class RepositoryFacade implements InvocationHandler {

    private static Logger LOGGER = LoggerFactory.getLogger(DynamicInvocationHandler.class);

    private final Class<? extends IRepository<?,?>> repositoryClazz;

    private final Class<?> idClazz;

    private final Class<? extends AbstractEntity<?>> entityClazz;

    // DBMS components
    private Analyzer analyzer;
    private Planner planner;

    public RepositoryFacade(final Class<? extends IRepository<?,?>> repositoryClazz){
        this.repositoryClazz = repositoryClazz;

        ParameterizedTypeImpl typeImpl = ((ParameterizedTypeImpl) repositoryClazz.
                getGenericInterfaces()[0]);

        Type[] types = typeImpl.getActualTypeArguments();

        this.entityClazz = (Class<? extends AbstractEntity<?>>) types[1];
        this.idClazz = (Class<?>) types[0];
    }

    public void setAnalyzer(final Analyzer analyzer){
        this.analyzer = analyzer;
    }

    public void setPlanner(final Planner planner){
        this.planner = planner;
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

                // acts as a single transaction, so all coinstraints, of every single row must be present

                PlanNode node = planner.planBulkInsert(); //(args[0]);

                break;
            }
            case "fetch": {

                // dispatch to analyzer passing the clazz param
                QueryTree queryTree = analyzer.analyze( (IStatement) args[0], (Class<?>) args[1]);
                PlanNode node = planner.plan( queryTree );

                // get concrete executor from application metadata
                SequentialQueryExecutor queryExecutor = new SequentialQueryExecutor(node);

                OperatorResult result = queryExecutor.get();
                return result.getDataTransferObjects().size() > 1 ?
                        result.getDataTransferObjects() : result.getDataTransferObjects().get(0);

            }
            default: throw new IllegalStateException("Unknown repository operation.");
        }

        return null;
    }
}

