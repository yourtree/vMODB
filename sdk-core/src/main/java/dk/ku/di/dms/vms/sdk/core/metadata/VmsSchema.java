package dk.ku.di.dms.vms.sdk.core.metadata;

import dk.ku.di.dms.vms.sdk.core.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.common.meta.ConstraintReference;
import dk.ku.di.dms.vms.modb.common.meta.DataType;

/**
 * The <code>VmsSchema</code> record describes the schema of {@link VmsTable}.
 */
public record VmsSchema (

    // identification of columns that form the primary key. all tables must have a primary key
    int[] primaryKeyColumns,

    // the name of the columns
    String[] columnNames,

    // the data types of the columns
    DataType[] columnDataTypes,

    // foreign key references
    ForeignKeyReference[] foreignKeyReferences,

    // constraints, referred by column position
    ConstraintReference[] constraintReferences

    // the constraints of this schema, where key: column position and value is the actual constraint
    // Map<Integer, ConstraintReference> constraintMap,

    // basically a map of column name to exact position in row values
    // Map<String,Integer> columnPositionMap
    ){}

//    public VmsSchema(final String[] columnNames, final DataType[] columnDataTypes, final int[] primaryKeyColumns, final ConstraintReference[] constraints) {
//        this.columnNames = columnNames;
//        this.columnDataTypes = columnDataTypes;
//        int size = columnNames.length;
//        this.columnPositionMap = new HashMap<>(size);
//        // build index map
//        for(int i = 0; i < size; i++){
//            columnPositionMap.put(columnNames[i],i);
//        }
//        this.primaryKeyColumns = primaryKeyColumns;
//        addConstraints( constraints );
//    }

//    private void addConstraints( final ConstraintReference[] constraints ){
//
//        if(constraints != null && constraints.length > 0){
//            this.constraintMap = new HashMap<>( constraints.length );
//            for( ConstraintReference constraintReference : constraints ){
//                this.constraintMap.put( constraintReference.column, constraintReference );
//            }
//        }
//
//    }

//    public int[] buildColumnPositionArray(final String[] columnList){
//        final int[] columnPosArray = new int[ columnList.length ];
//        for(int j = 0; j < columnList.length; j++){
//            columnPosArray[j] = getColumnPosition( columnList[j] );
//        }
//        return columnPosArray;
//    }

}
