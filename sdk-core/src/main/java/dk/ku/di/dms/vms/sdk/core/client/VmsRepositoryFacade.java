package dk.ku.di.dms.vms.sdk.core.client;

import dk.ku.di.dms.vms.modb.common.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.common.interfaces.IRepository;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class VmsRepositoryFacade implements IVmsRepositoryFacade, InvocationHandler {

    private final Class<?> pkClazz;

    private final Class<? extends IEntity<?>> entityClazz;

    @SuppressWarnings("unchecked")
    public VmsRepositoryFacade(final Class<? extends IRepository<?,?>> repositoryClazz){
        Type[] types = ((ParameterizedType) repositoryClazz.getGenericInterfaces()[0]).getActualTypeArguments();
        this.pkClazz = (Class<?>) types[0];
        this.entityClazz = (Class<? extends IEntity<?>>) types[1];
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // String methodName = ;

        switch(method.getName()){

            case "insert" -> {
                // TODO receive a plan tree from the planner
                break;
            }
            case "insertAll" -> {



                break;
            }
            case "fetch" -> {



            }
            // default: throw new IllegalStateException("Unknown repository operation.");
        }

        return null;
    }

}
