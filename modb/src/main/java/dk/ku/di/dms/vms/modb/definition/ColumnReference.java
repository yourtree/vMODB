package dk.ku.di.dms.vms.modb.definition;

import dk.ku.di.dms.vms.modb.common.type.DataType;

public final class ColumnReference {

    // used to set the value of the attribute of a return class type
    public final String columnName;

    public final int columnPosition;

    public final DataType dataType;

    public final Table table;

    public ColumnReference(final String columnName, final int columnPosition, final Table table) {
        this.columnName = columnName;
        this.columnPosition = columnPosition;
        this.dataType = table.getSchema().getColumnDataType(columnPosition);
        this.table = table;
    }

    public ColumnReference(final String columnName, final Table table) {
        this.columnName = columnName;
        this.columnPosition = table.getSchema().getColumnPosition( columnName );
        this.dataType = table.getSchema().getColumnDataType(columnPosition);
        this.table = table;
    }

}
