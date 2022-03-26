package dk.ku.di.dms.vms.modb.common.query.statement;

import dk.ku.di.dms.vms.modb.common.query.clause.SetClauseElement;

import java.util.List;

public class UpdateStatement extends AbstractStatement {

    public String table;

    public List<SetClauseElement> setClause;

    @Override
    public UpdateStatement getAsUpdateStatement() {
        return this;
    }

    @Override
    public boolean isUpdate(){ return true; }

}
