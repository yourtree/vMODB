package dk.ku.di.dms.vms.sdk.core.facade;

import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

/**
 * The default repository facade does not contain references to DBMS components
 * since the application is not co-located with the MODB
 */
public class DefaultRepositoryFacade implements IVmsRepositoryFacade, InvocationHandler {


    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        return null;
    }


    @Override
    public Object fetch(SelectStatement selectStatement, Type type) {
        return null;
    }
}
