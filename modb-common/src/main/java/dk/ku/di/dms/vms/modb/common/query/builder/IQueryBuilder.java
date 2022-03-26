package dk.ku.di.dms.vms.modb.common.query.builder;

import dk.ku.di.dms.vms.modb.common.query.statement.IStatement;

public interface IQueryBuilder<T extends IStatement> {
    T build();
}
