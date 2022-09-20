package dk.ku.di.dms.vms.sdk.core.facade;

import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * The default repository facade does not contain references to DBMS components
 * since the application is not co-located with the MODB
 */
public class DefaultRepositoryFacade implements IVmsRepositoryFacade, InvocationHandler {

    private final Class<?> pkClazz;

    private final Class<? extends IEntity<?>> entityClazz;

    @SuppressWarnings({"unchecked"})
    public DefaultRepositoryFacade(Class<? extends IRepository<?,?>> repositoryClazz){

        Type[] types = ((ParameterizedType) repositoryClazz.getGenericInterfaces()[0]).getActualTypeArguments();

        this.entityClazz = (Class<? extends IEntity<?>>) types[1];
        this.pkClazz = (Class<?>) types[0];

    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        return null;
    }


    @Override
    public Object fetch(SelectStatement selectStatement, Type type) {
        return null;
    }

    @Override
    public InvocationHandler asInvocationHandler() {
        return this;
    }

}
