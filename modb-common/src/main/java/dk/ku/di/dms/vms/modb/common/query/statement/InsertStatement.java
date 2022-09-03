package dk.ku.di.dms.vms.modb.common.query.statement;

public class InsertStatement implements IStatement {

    public Object[] values;

    public String table;

    @Override
    public StatementType getType() {
        return StatementType.INSERT;
    }

    @Override
    public InsertStatement asInsertStatement() {
        return this;
    }

}
