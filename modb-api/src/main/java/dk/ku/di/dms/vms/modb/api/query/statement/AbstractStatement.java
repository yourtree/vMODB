package dk.ku.di.dms.vms.modb.api.query.statement;

import dk.ku.di.dms.vms.modb.api.query.clause.WhereClauseElement;

import java.util.List;

public sealed abstract class AbstractStatement implements IStatement
        permits SelectStatement, UpdateStatement, DeleteStatement {

    // TODO make this a string
    // used to cache query plans. where clause only found in select, update, and delete
    public StringBuilder SQL = new StringBuilder();

    public List<WhereClauseElement> whereClause;

}
