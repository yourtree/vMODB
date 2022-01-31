package dk.ku.di.dms.vms.database.store;

import java.lang.reflect.Field;

public class Column {

    public final String name;
    public final ColumnType type;
    public final int hashCode;

    // not sure right whether this should be in column or column reference or another class...
    public final Field field;

    public Column(final String name, final ColumnType type, final int hashCode, final Field field) {
        this.name = name;
        this.type = type;
        this.hashCode = hashCode;
        this.field = field;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
