package dk.ku.di.dms.vms.database.store.meta;

import dk.ku.di.dms.vms.database.store.table.Table;

import java.util.HashMap;
import java.util.Map;

/**
 * The <code>Schema</code> class describes the schema of {@link Table}.
 */
public class Schema {

    // identification of columns that form the primary key. all tables must have a primary key
    private int[] primaryKeyColumns;

    private ForeignKeyReference[] foreignKeys;

    // the name of the columns
    private final String[] columnNames;

    // the data types of the columns
    private final DataType[] columnDataTypes;

    // the constraints of this schema
    private final ConstraintReference[] constraints;

    // basically a map of column name to exact position in row values
    private final Map<String,Integer> columnPositionMap;

    // probably later should build a foreign key map
    // private final Map<Integer,>

    public Integer getColumnPosition(String columnName){
        return columnPositionMap.get(columnName);
    }

    public String getColumnName(int columnPosition){
        return columnNames[columnPosition];
    }

    public DataType getColumnDataType(int columnIndex){
        return columnDataTypes[columnIndex];
    }

    public int[] getPrimaryKeyColumns(){
        return this.primaryKeyColumns;
    }

    public Schema(final String[] columnNames, final DataType[] columnDataTypes, final int[] primaryKeyColumns) {
        this.columnNames = columnNames;
        this.columnDataTypes = columnDataTypes;
        int size = columnNames.length;
        this.columnPositionMap = new HashMap<>(size);
        // build index map
        for(int i = 0; i < size; i++){
            columnPositionMap.put(columnNames[i],i);
        }
        this.primaryKeyColumns = primaryKeyColumns;
        this.constraints = null;
    }

    public Schema(final String[] columnNames, final DataType[] columnDataTypes, final int[] primaryKeyColumns, final ConstraintReference[] constraints) {
        this.columnNames = columnNames;
        this.columnDataTypes = columnDataTypes;
        int size = columnNames.length;
        this.columnPositionMap = new HashMap<>(size);
        // build index map
        for(int i = 0; i < size; i++){
            columnPositionMap.put(columnNames[i],i);
        }
        this.primaryKeyColumns = primaryKeyColumns;
        this.constraints = constraints;
    }

    public int[] buildColumnPositionArray(final String[] columnList){
        final int[] columnPosArray = new int[ columnList.length ];
        for(int j = 0; j < columnList.length; j++){
            columnPosArray[j] = getColumnPosition( columnList[j] );
        }
        return columnPosArray;
    }

}
