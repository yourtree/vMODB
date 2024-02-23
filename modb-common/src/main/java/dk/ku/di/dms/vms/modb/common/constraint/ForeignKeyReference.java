package dk.ku.di.dms.vms.modb.common.constraint;

public class ForeignKeyReference {
    // this is always part of the same virtual microservice
    private final String parentTableName;

    private final String parentColumnName;

    private final int columnIndex;

    public ForeignKeyReference(String parentTableName, String parentColumnName, int columnIndex) {
        this.parentTableName = parentTableName;
        this.parentColumnName = parentColumnName;
        this.columnIndex = columnIndex;
    }

    public String vmsTableName(){
        return this.parentTableName;
    }

    public String columnName(){
        return this.parentColumnName;
    }

    public int columnIndex(){
        return columnIndex;
    }

}