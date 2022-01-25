package dk.ku.di.dms.vms.database.store;

public class Column {

    public String name;

    public ColumnType type;

    @Override
    public int hashCode() {
        // TODO return number based on ASCII
        return super.hashCode();
    }
}
