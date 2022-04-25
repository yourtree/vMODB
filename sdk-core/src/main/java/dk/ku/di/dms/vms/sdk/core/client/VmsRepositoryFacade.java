package dk.ku.di.dms.vms.sdk.core.client;

import dk.ku.di.dms.vms.modb.common.event.DataRequestEvent;
import dk.ku.di.dms.vms.modb.common.event.DataResponseEvent;
import dk.ku.di.dms.vms.modb.common.interfaces.IDTO;
import dk.ku.di.dms.vms.modb.common.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.common.interfaces.IRepository;
import dk.ku.di.dms.vms.modb.common.query.statement.IStatement;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Queue;

/**
 * A facade class that only handles data-related functionality
 */
public class VmsRepositoryFacade implements IVmsRepositoryFacade, InvocationHandler {

    private final Class<?> pkClazz;

    private final Class<? extends IEntity<?>> entityClazz;

    private final Queue<DataRequestEvent> requestQueue;

    private final Map<Long,DataResponseEvent> responseMap;

    @SuppressWarnings("unchecked")
    public VmsRepositoryFacade(Class<? extends IRepository<?,?>> repositoryClazz, Queue<DataRequestEvent> requestQueue, Map<Long,DataResponseEvent> responseMap){
        Type[] types = ((ParameterizedType) repositoryClazz.getGenericInterfaces()[0]).getActualTypeArguments();
        this.pkClazz = (Class<?>) types[0];
        this.entityClazz = (Class<? extends IEntity<?>>) types[1];
        this.requestQueue = requestQueue;
        this.responseMap = responseMap;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        String methodName = method.getName();
        Thread currentThread = Thread.currentThread();

        // inserts, updates, and deletes are non-blocking by default
        // queries, on the other side, require blocking the thread

        switch(methodName){

            case "insert": {
                // TODO receive a plan tree from the planner
                //  use entityClazz to retrieve the schema and retrieve the values via method handles
                break;
            }
            case "insertAll": {

                // currentThread.wait();
                // create a data request, register on the queue

                // data request { thread id, and the statement (in object-oriented parsed mode)



                break;
            }
            case "fetchMany":
            case "fetchOne": {
                // args[0] is the statement, args[1] is the
                long identifier = currentThread.getId();

                boolean isDTO = false;
                if( args[1] instanceof IDTO ) {
                    isDTO = true;
                }

                DataRequestEvent dataRequestEvent = new DataRequestEvent( identifier, methodName, (IStatement) args[0], isDTO );

                requestQueue.add( dataRequestEvent );

                // now wait until event handler wakes me up
                synchronized (dataRequestEvent){
                    dataRequestEvent.wait();
                }

                // woke up
                DataResponseEvent response = responseMap.remove( identifier );

                return response.result;

            }

            case "issue": {

            }
            // default: throw new IllegalStateException("Unknown repository operation.");
            default: { throw new IllegalStateException("Unexpected value: " + methodName); }
        }

        return null;
    }

}
