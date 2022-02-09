package dk.ku.di.dms.vms.database.store;

public class ColumnReference {

    public final int columnIndex;

    public final Table table;

    // rename? we don't need this for now, unless it is a dto....

    public ColumnReference(final int columnIndex, final Table table) {
        this.columnIndex = columnIndex;
        this.table = table;
    }

}
