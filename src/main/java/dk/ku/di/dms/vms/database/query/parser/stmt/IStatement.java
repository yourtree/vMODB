package dk.ku.di.dms.vms.database.query.parser.stmt;

public interface IStatement {

    default SelectStatement getAsSelectStatement(){
        return null;
    }

    default UpdateStatement getAsUpdateStatement(){
        return null;
    }

}
