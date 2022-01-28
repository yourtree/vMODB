package dk.ku.di.dms.vms.database.query.parser.stmt;

import java.util.ArrayList;
import java.util.List;

public class UpdateStatement extends AbstractStatement {

    public String table;

    public List<SetClauseElement> setClause;

    public UpdateStatement() {
        this.setClause = new ArrayList<>();
        this.whereClause = new ArrayList<>();
    }
}
