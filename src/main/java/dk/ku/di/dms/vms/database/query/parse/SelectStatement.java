package dk.ku.di.dms.vms.database.query.parse;

import java.util.List;

public class SelectStatement implements Statement {

    public List<String> projection;

    public List<String> fromClause;

    public List<WhereClauseElement> whereClause;

    public List<String> sortClause;

    public List<String> groupClause;

}
