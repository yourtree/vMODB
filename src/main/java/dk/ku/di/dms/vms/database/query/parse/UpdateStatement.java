package dk.ku.di.dms.vms.database.query.parse;

import java.util.List;

public class UpdateStatement implements Statement {

    public String table;

    public List<SetClauseElement> setClause;

    public List<WhereClauseElement> whereClause;

}
