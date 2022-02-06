package dk.ku.di.dms.vms.database.store.refac;

import dk.ku.di.dms.vms.database.store.ColumnType;

public class Column {

    public final String name;
    public final ColumnType type;

    public Column(String name, ColumnType type) {
        this.name = name;
        this.type = type;
    }
}
