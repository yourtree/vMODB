package dk.ku.di.dms.vms.database.store.meta;

import dk.ku.di.dms.vms.database.store.table.Table;

public class ColumnReference {

    public final int columnPosition;

    public final DataType dataType;

    public final Table table;

    public ColumnReference(int columnPosition, DataType dataType, Table table) {
        this.columnPosition = columnPosition;
        this.dataType = dataType;
        this.table = table;
    }
}
