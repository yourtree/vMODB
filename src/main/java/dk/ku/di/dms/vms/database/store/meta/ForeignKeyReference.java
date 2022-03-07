package dk.ku.di.dms.vms.database.store.meta;

import dk.ku.di.dms.vms.database.store.table.Table;

public class ForeignKeyReference {

    private Table table;

    // the position of the columns that reference the other table
    private int[] columnSet;

    private CardinalityTypeEnum cardinality;

    public ForeignKeyReference(Table table, int[] columnSet) {
        this.table = table;
        this.columnSet = columnSet;
    }

}
