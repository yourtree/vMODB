package dk.ku.di.dms.vms.database.query.parser.stmt;

import java.util.List;

public class UpdateStatement implements IStatement {

    public String table;

    public List<SetClauseElement> setClause;

    public List<WhereClauseElement> whereClause;

}
