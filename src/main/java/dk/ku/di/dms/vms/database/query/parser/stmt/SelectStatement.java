package dk.ku.di.dms.vms.database.query.parser.stmt;

import java.util.List;

public class SelectStatement extends AbstractStatement {

    public List<String> columns;

    public List<String> fromClause;

    public List<SortClauseElement> sortClause;

    public List<String> groupClause;

}
