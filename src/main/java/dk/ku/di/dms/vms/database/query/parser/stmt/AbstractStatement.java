package dk.ku.di.dms.vms.database.query.parser.stmt;

import dk.ku.di.dms.vms.database.query.parser.clause.JoinClauseElement;
import dk.ku.di.dms.vms.database.query.parser.clause.WhereClauseElement;

import java.util.List;

public abstract class AbstractStatement implements IStatement {

    public List<WhereClauseElement<?>> whereClause;

    public List<JoinClauseElement> joinClause;

}
