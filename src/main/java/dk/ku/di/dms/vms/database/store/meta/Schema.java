package dk.ku.di.dms.vms.database.store.meta;

import dk.ku.di.dms.vms.database.store.table.Table;

import java.util.Hashtable;
import java.util.Map;

/**
 * The <code>Schema</code> class describes the schema of {@link Table}.
 */
public class Schema {

    private final String[] columnNames;
    private final DataType[] columnDataTypes;

    // basically a map of column name to exact position in row values
    private final Map<String,Integer> columnIndexMap;

    public Integer getColumnIndex(String columnName){
        return columnIndexMap.get(columnName);
    }

    public DataType getColumnDataType(int columnIndex){
        return columnDataTypes[columnIndex];
    }

    public Schema(String[] columnNames, DataType[] columnDataTypes) {
        this.columnNames = columnNames;
        this.columnDataTypes = columnDataTypes;
        int size = columnNames.length;
        this.columnIndexMap = new Hashtable<>(size);
        // build index map
        for(int i = 0; i < size; i++){
            columnIndexMap.put(columnNames[i],i);
        }
    }



}
