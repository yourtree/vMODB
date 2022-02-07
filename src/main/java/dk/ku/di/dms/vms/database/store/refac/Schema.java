package dk.ku.di.dms.vms.database.store.refac;

import dk.ku.di.dms.vms.database.store.DataType;

/**
 * The <code>Schema</code> class describes the schema of {@link Table}.
 */
public class Schema {

    private final String[] columnNames;
    private final DataType[] columnDataTypes;

    public Schema(String[] columnNames, DataType[] columnDataTypes) {
        this.columnNames = columnNames;
        this.columnDataTypes = columnDataTypes;
    }

}
