package dk.ku.di.dms.vms.database.store;

public class ColumnReference {

    public Column column;
    public Table<?,?> table;

    // rename? we dont need this for now, unless it is a dto....

    public ColumnReference(Column column, Table<?,?> table) {
        this.column = column;
        this.table = table;
    }

}
