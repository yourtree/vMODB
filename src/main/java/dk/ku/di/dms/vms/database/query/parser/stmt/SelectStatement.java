package dk.ku.di.dms.vms.database.query.parser.stmt;

import java.util.List;

public class SelectStatement implements IStatement {

    public List<String> columns;

    public List<String> fromClause;

    public List<JoinClauseElement> joinClause;

    public List<WhereClauseElement> whereClause;

    public List<SortClauseElement> sortClause;

    public List<String> groupClause;

}
