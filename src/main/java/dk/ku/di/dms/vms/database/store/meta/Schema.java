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
    private Map<Integer,ForeignKeyReference> foreignKeyMap;

    // the name of the columns
    private final String[] columnNames;

    // the data types of the columns
    private final DataType[] columnDataTypes;

    // the constraints of this schema
    private Map<Integer,ConstraintReference> constraintMap;

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
        addConstraints( constraints );
    }

    private void addConstraints( final ConstraintReference[] constraints ){

        if(constraints != null && constraints.length > 0){
            this.constraintMap = new HashMap<>( constraints.length );
            for( ConstraintReference constraintReference : constraints ){
                this.constraintMap.put( constraintReference.column, constraintReference );
            }
        }

    }

    public void addForeignKeyConstraints(final List<ForeignKeyReference> foreignKeyReferences){
        if(foreignKeyReferences != null && foreignKeyReferences.size() > 0){
            this.foreignKeyMap = new HashMap<>();
            for(ForeignKeyReference foreignKeyReference : foreignKeyReferences){
                int columnPos =  foreignKeyReference.getColumnPosition();
                this.foreignKeyMap.putIfAbsent(columnPos, foreignKeyReference);
            }
        }
    }

    public ForeignKeyReference isForeignKey(final int columnPosition){
        return this.foreignKeyMap.getOrDefault(columnPosition, null);
    }

    public ConstraintReference isConstraint(final int columnPosition){
        return this.constraintMap.getOrDefault(columnPosition, null);
    }

    public int[] buildColumnPositionArray(final String[] columnList){
        final int[] columnPosArray = new int[ columnList.length ];
        for(int j = 0; j < columnList.length; j++){
            columnPosArray[j] = getColumnPosition( columnList[j] );
        }
        return columnPosArray;
    }

}
