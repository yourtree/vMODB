package dk.ku.di.dms.vms.database.store.meta;

import dk.ku.di.dms.vms.database.store.table.Table;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * The <code>Schema</code> class describes the schema of {@link Table}.
 */
public class Schema {

    // identification of columns that form the primary key. all tables must have a primary key
    private int[] primaryKey;

    private ForeignKeyReference[] foreignKeys;

    private final String[] columnNames;
    private final DataType[] columnDataTypes;

    // basically a map of column name to exact position in row values
    private final Map<String,Integer> columnPositionMap;

    public Integer getColumnIndex(String columnName){
        return columnPositionMap.get(columnName);
    }

    public DataType getColumnDataType(int columnIndex){
        return columnDataTypes[columnIndex];
    }

    public Schema( String[] columnNames, DataType[] columnDataTypes) {
        this.columnNames = columnNames;
        this.columnDataTypes = columnDataTypes;
        int size = columnNames.length;
        this.columnPositionMap = new HashMap<>(size);
        // build index map
        for(int i = 0; i < size; i++){
            columnPositionMap.put(columnNames[i],i);
        }
    }

}
