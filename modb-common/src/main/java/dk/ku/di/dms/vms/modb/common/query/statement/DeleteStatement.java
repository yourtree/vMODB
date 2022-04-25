package dk.ku.di.dms.vms.modb.common.query.statement;

import dk.ku.di.dms.vms.modb.common.query.clause.WhereClauseElement;

import java.util.List;

public class DeleteStatement implements IStatement {

    public String table;

    public List<WhereClauseElement<?>> whereClause;

}
