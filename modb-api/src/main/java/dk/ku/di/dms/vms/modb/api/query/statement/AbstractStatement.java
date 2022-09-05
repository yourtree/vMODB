package dk.ku.di.dms.vms.modb.api.query.statement;

import dk.ku.di.dms.vms.modb.api.query.clause.WhereClauseElement;

import java.util.List;

public abstract sealed class AbstractStatement
        implements IStatement permits SelectStatement, UpdateStatement, DeleteStatement {

    // used later to cache query plans. where clause only found in select, update, and delete
    public StringBuilder SQL = new StringBuilder();

    public List<WhereClauseElement<?>> whereClause;

}
