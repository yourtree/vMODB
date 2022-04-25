package dk.ku.di.dms.vms.modb.common.meta;

/**
 * The <code>VmsSchema</code> record describes the schema of VmsTable.
 */
public record VmsDataSchema(

    String tableName,

    // identification of columns that form the primary key. all tables must have a primary key
    int[] primaryKeyColumns,

    // the name of the columns
    String[] columnNames,

    // the data types of the columns
    DataType[] columnDataTypes,

    // foreign key references
    ForeignKeyReference[] foreignKeyReferences, // this can be outside, other vms

    // constraints, referred by column position
    ConstraintReference[] constraintReferences

){}