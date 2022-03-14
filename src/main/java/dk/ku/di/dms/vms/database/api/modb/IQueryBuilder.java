package dk.ku.di.dms.vms.database.api.modb;

import dk.ku.di.dms.vms.database.query.parser.stmt.IStatement;

public interface IQueryBuilder<T extends IStatement> {
    T build();
}
