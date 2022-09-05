package dk.ku.di.dms.vms.sdk.core.facade;

import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;

import java.lang.reflect.Type;

public interface IVmsRepositoryFacade {

    Object fetch(SelectStatement selectStatement, Type type);

}
