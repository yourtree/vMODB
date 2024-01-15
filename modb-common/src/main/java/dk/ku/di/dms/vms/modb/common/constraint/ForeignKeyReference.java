package dk.ku.di.dms.vms.modb.common.constraint;

public class ForeignKeyReference {
    // this is always part of the same virtual microservice
    public String vmsTableName;

    public String columnName;

    public ForeignKeyReference(){}

    public ForeignKeyReference(String vmsTableName, String columnName) {
        this.vmsTableName = vmsTableName;
        this.columnName = columnName;
    }

    public String vmsTableName(){
        return this.vmsTableName;
    }

    public String columnName(){
        return this.columnName;
    }

}