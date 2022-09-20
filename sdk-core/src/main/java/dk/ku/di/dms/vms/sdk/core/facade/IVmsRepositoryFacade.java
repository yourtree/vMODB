package dk.ku.di.dms.vms.sdk.core.facade;

import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Type;

public interface IVmsRepositoryFacade extends InvocationHandler {

    Object fetch(SelectStatement selectStatement, Type type);

}
