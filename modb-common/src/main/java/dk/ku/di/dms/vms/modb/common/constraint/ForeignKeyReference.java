package dk.ku.di.dms.vms.modb.common.constraint;

public class ForeignKeyReference {
    // this is always part of the same virtual microservice
    private final String parentTableName;

    private final String parentColumnName;

    public ForeignKeyReference(String parentTableName, String parentColumnName) {
        this.parentTableName = parentTableName;
        this.parentColumnName = parentColumnName;
    }

    public String vmsTableName(){
        return this.parentTableName;
    }

    public String columnName(){
        return this.parentColumnName;
    }

}