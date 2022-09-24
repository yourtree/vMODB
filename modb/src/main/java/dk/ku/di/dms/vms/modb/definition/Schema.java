package dk.ku.di.dms.vms.modb.definition;

import dk.ku.di.dms.vms.modb.common.constraint.ConstraintReference;
import dk.ku.di.dms.vms.modb.common.type.Constants;
import dk.ku.di.dms.vms.modb.common.type.DataType;

import java.util.HashMap;
import java.util.Map;

/**
 * The <code>Schema</code> class describes the schema of {@link Table}.
 */
public class Schema {

    // flag active + materialized hashed PK
    private static final int recordHeader = Header.SIZE + Integer.BYTES;

    // identification of columns that form the primary key. all tables must have a primary key
    private final int[] primaryKeyColumns;

    // the name of the columns
    private final String[] columnNames;

    // the data types of the columns
    private final DataType[] columnDataTypes;

    // offset of the column in a buffer
    private final int[] columnOffset;

    // the constraints of this schema, where key: column position and value is the actual constraint
    private Map<Integer, ConstraintReference> constraintMap;

    // basically a map of column name to exact position in row values
    private final Map<String, Integer> columnPositionMap;

    private final int recordSize; // the sum of all possible data types

    public Schema(final String[] columnNames, final DataType[] columnDataTypes, final int[] primaryKeyColumns, final ConstraintReference[] constraints) {
        this.columnNames = columnNames;
        this.columnDataTypes = columnDataTypes;

        this.columnOffset = new int[columnDataTypes.length];

        int acc = recordHeader;
        for(int j = 0; j < columnDataTypes.length; j++){
            switch (columnDataTypes[j]){
                case LONG, DATE -> acc += Long.BYTES;
                case CHAR -> acc += Constants.DEFAULT_MAX_SIZE_CHAR;
                case INT -> acc += Integer.BYTES;
                case FLOAT -> acc += Float.BYTES;
                case DOUBLE -> acc += Double.BYTES;
                // case BOOL -> acc += 1;
            }
            columnOffset[j] = acc;
        }

        this.recordSize = acc;

        int size = columnNames.length;
        this.columnPositionMap = new HashMap<>(size);
        // build index map
        for(int i = 0; i < size; i++){
            columnPositionMap.put(columnNames[i],i);
        }
        this.primaryKeyColumns = primaryKeyColumns;
        addConstraints( constraints );
    }

    public Integer getColumnPosition(String columnName){
        return this.columnPositionMap.get(columnName);
    }

    public String[] getColumnNames(){
        return this.columnNames;
    }

    public DataType getColumnDataType(int columnIndex){
        return this.columnDataTypes[columnIndex];
    }

    public int[] getPrimaryKeyColumns(){
        return this.primaryKeyColumns;
    }

    public Map<Integer, ConstraintReference> constraints(){
        return this.constraintMap;
    }

    public int getColumnOffset(int columnIndex){
        return this.columnOffset[columnIndex];
    }

    public int[] columnOffset(){
        return this.columnOffset;
    }

    public int getRecordSize(){
        return this.recordSize;
    }

    public int getRecordSizeWithoutHeader(){
        return this.recordSize - recordHeader;
    }

    private void addConstraints( ConstraintReference[] constraints ){

        if(constraints != null && constraints.length > 0){
            this.constraintMap = new HashMap<>( constraints.length );
            for( ConstraintReference constraintReference : constraints ){
                this.constraintMap.put( constraintReference.column, constraintReference );
            }
        }

    }

    public int[] buildColumnPositionArray(String[] columnList){
        final int[] columnPosArray = new int[ columnList.length ];
        for(int j = 0; j < columnList.length; j++){
            columnPosArray[j] = getColumnPosition( columnList[j] );
        }
        return columnPosArray;
    }

}
