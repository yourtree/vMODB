package dk.ku.di.dms.vms.database.store.meta;

import dk.ku.di.dms.vms.database.store.table.Table;

public class ForeignKeyReference {

    private final Table table;

    // the position of the columns that reference the other table
    private final int columnPosition;

    // private CardinalityTypeEnum cardinality; TODO check whether this is necessary

    public ForeignKeyReference(final Table table, final int columnPosition) {
        this.table = table;
        this.columnPosition = columnPosition;
    }

    public Table getTable() {
        return table;
    }

    public int getColumnPosition() {
        return columnPosition;
    }

}
