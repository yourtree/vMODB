package dk.ku.di.dms.vms.sdk.core.client;

import dk.ku.di.dms.vms.modb.common.event.DataRequestEvent;
import dk.ku.di.dms.vms.modb.common.event.DataResponseEvent;
import dk.ku.di.dms.vms.modb.common.interfaces.IDTO;
import dk.ku.di.dms.vms.modb.common.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.common.interfaces.IRepository;
import dk.ku.di.dms.vms.web_common.runnable.VMSFutureTask;
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

                // https://docs.oracle.com/en/java/javase/16/docs/api/java.base/java/util/concurrent/SynchronousQueue.html
                // is the cost of instantiating a sync queue for every data request an overhead?
                //   instantiate an object compared to synchronize objects is irrelevant in terms of overhead,
                //   cost less than waiting a semaphore
                // semantics of wait/notify vary across JDKs

//                SynchronousQueue<DataResponseEvent> queue = new SynchronousQueue<>();
//                queue.take(); // how the thread is handled by the jvm, if they put back in the pool or not

                // now wait until event handler wakes me up
                synchronized (dataRequestEvent){

                    // future.get() // i don't know whether this thread will be available for other tasks..
                    // i believe with wait this thread goes back to the pool.. but i have to check
                    // TODO investigate this further
                    dataRequestEvent.wait(); // this is asynchronous nature
                }

                // woke up
                DataResponseEvent response = responseMap.remove( identifier );

                return response.result;

            }
            case "fetchOnePromise" : {
                VMSFutureTask<IDTO> futureTask = new VMSFutureTask<IDTO>(currentThread);
            }
            case "issue": {

            }
            // default: throw new IllegalStateException("Unknown repository operation.");
            default: { throw new IllegalStateException("Unexpected value: " + methodName); }
        }

        return null;
    }

}
