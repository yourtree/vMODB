package dk.ku.di.dms.vms.sdk.core.facade;

import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Type;
import java.util.List;

public interface IVmsRepositoryFacade {

    Object fetch(SelectStatement selectStatement, Type type);

    Object fetch(SelectStatement selectStatement);

    void insertAll(List<Object> entities);

    InvocationHandler asInvocationHandler();

}
