package dk.ku.di.dms.vms.modb.common.query.statement;

public interface IStatement {

    default boolean isSelect(){ return false; }

    default SelectStatement getAsSelectStatement(){
        return null;
    }

    default boolean isUpdate(){ return false; }

    default UpdateStatement getAsUpdateStatement(){
        return null;
    }

}
