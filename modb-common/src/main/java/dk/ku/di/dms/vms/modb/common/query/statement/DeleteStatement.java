package dk.ku.di.dms.vms.modb.common.query.statement;

public final class DeleteStatement extends AbstractStatement {

    public String table;

    @Override
    public StatementType getType() {
        return StatementType.DELETE;
    }

    @Override
    public DeleteStatement asDeleteStatement() {
        return this;
    }

}
