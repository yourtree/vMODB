package dk.ku.di.dms.vms.modb.common.query.statement;

import dk.ku.di.dms.vms.modb.common.query.clause.JoinClauseElement;
import dk.ku.di.dms.vms.modb.common.query.clause.WhereClauseElement;

import java.util.List;

public abstract class AbstractStatement implements IStatement {

    public List<WhereClauseElement<?>> whereClause;

    public List<JoinClauseElement> joinClause;

}
