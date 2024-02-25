package dk.ku.di.dms.vms.modb.common.constraint;

public class ForeignKeyReference {
    // this is always part of the same virtual microservice
    private final String parentTableName;

    private final String parentColumnName;

    private final String localColumnName;

    public ForeignKeyReference(String parentTableName, String parentColumnName, String localColumnName) {
        this.parentTableName = parentTableName;
        this.parentColumnName = parentColumnName;
        this.localColumnName = localColumnName;
    }

    public String parentTableName(){
        return this.parentTableName;
    }

    public String parentColumnName(){
        return this.parentColumnName;
    }

    public String localColumnName(){
        return this.localColumnName;
    }

}