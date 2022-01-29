package dk.ku.di.dms.vms.database.store;

public class Column {

    public final String name;
    public final ColumnType type;
    public final int hashCode;

    public Column(String name, ColumnType type, int hashCode) {
        this.name = name;
        this.type = type;
        this.hashCode = hashCode;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
