package dk.ku.di.dms.vms.database.query.parser.stmt;

import dk.ku.di.dms.vms.database.query.parser.clause.SetClauseElement;

import java.util.List;

public class UpdateStatement extends AbstractStatement {

    public String table;

    public List<SetClauseElement> setClause;

}
