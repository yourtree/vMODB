package dk.ku.di.dms.vms.database.store;

public class ColumnReference {

    public final Column column;
    public final Table<?,?> table;

    // rename? we don't need this for now, unless it is a dto....

    public ColumnReference(final Column column, final Table<?,?> table) {
        this.column = column;
        this.table = table;
    }

}
