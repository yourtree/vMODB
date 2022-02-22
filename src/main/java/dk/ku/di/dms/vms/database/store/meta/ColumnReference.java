package dk.ku.di.dms.vms.database.store.meta;

import dk.ku.di.dms.vms.database.store.table.Table;

public class ColumnReference {

    public final int columnIndex;

    public final DataType dataType;

    public final Table table;

    public ColumnReference(int columnIndex, DataType dataType, Table table) {
        this.columnIndex = columnIndex;
        this.dataType = dataType;
        this.table = table;
    }
}
