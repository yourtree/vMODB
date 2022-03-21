package dk.ku.di.dms.vms.database.store.meta;

import dk.ku.di.dms.vms.database.store.table.Table;

import java.util.*;

/**
 * The <code>Schema</code> class describes the schema of {@link Table}.
 */
public class Schema {

    // identification of columns that form the primary key. all tables must have a primary key
    private final int[] primaryKeyColumns;

    // foreign key map: key: column position value: foreign key reference
    private final Map<Integer,List<ForeignKeyReference>> foreignKeyMap;

    // the name of the columns
    private final String[] columnNames;

    // the data types of the columns
    private final DataType[] columnDataTypes;

    // the constraints of this schema
    private final ConstraintReference[] constraints;

    // basically a map of column name to exact position in row values
    private final Map<String,Integer> columnPositionMap;

    public Integer getColumnPosition(String columnName){
        return columnPositionMap.get(columnName);
    }

    public String[] getColumnNames(){
        return columnNames;
    }

    public DataType getColumnDataType(int columnIndex){
        return columnDataTypes[columnIndex];
    }

    public int[] getPrimaryKeyColumns(){
        return this.primaryKeyColumns;
    }

    public Schema(final String[] columnNames, final DataType[] columnDataTypes, final int[] primaryKeyColumns, final ForeignKeyReference[] foreignKeyColumns, final ConstraintReference[] constraints) {
        this.columnNames = columnNames;
        this.columnDataTypes = columnDataTypes;
        int size = columnNames.length;
        this.columnPositionMap = new HashMap<>(size);
        // build index map
        for(int i = 0; i < size; i++){
            columnPositionMap.put(columnNames[i],i);
        }
        this.primaryKeyColumns = primaryKeyColumns;

        if(foreignKeyColumns != null){
            this.foreignKeyMap = new HashMap<>(foreignKeyColumns.length);
            for(ForeignKeyReference foreignKeyReference : foreignKeyColumns){
                List<ForeignKeyReference> list;
                int columnPos = columnPositionMap.get( foreignKeyReference.getColumnName() );
                if(foreignKeyMap.get( columnPos ) == null){
                    list = new ArrayList<>(2); // usual number of foreign keys per table
                    foreignKeyMap.put( columnPos, list );
                } else {
                    list = foreignKeyMap.get( columnPos );
                }
                list.add(foreignKeyReference);
            }
        } else {
            this.foreignKeyMap = null;
        }
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
