package dk.ku.di.dms.vms.database.api.modb;

import dk.ku.di.dms.vms.infra.AbstractEntity;
import dk.ku.di.dms.vms.infra.IRepository;
import dk.ku.di.dms.vms.proxy.DynamicInvocationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

public class RepositoryFacade implements InvocationHandler {

    private static Logger LOGGER = LoggerFactory.getLogger(DynamicInvocationHandler.class);

    final private Class<? extends IRepository> repositoryClazz;

    final private Class<?> idClazz;
    final private Class<? extends AbstractEntity> entityClazz;

    public RepositoryFacade(final Class<? extends IRepository> repositoryClazz){
        this.repositoryClazz = repositoryClazz;

        ParameterizedTypeImpl typeImpl = ((ParameterizedTypeImpl) repositoryClazz.
                getGenericInterfaces()[0]);

        Type[] types = typeImpl.getActualTypeArguments();

        this.entityClazz = (Class<? extends AbstractEntity>) types[1];
        this.idClazz = (Class<?>) types[0];
    }

    // manage the connection of each method, making sure all connections are released
    //  at the end of the method

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
        LOGGER.info("Invoked method: {}", method.getName());

        String methodName = method.getName();


        switch(methodName){

            case "insert": {
                // TODO receive a plan tree from the planner
                break;
            }
            case "fetch":
                break;

            default: throw new Exception("Unknown repository operation.");
        }

//        JsonObject jsonObject = new JsonObject();
//        jsonObject.add("result",new JsonPrimitive("success"));
//        return jsonObject;
        return null;
    }
}

