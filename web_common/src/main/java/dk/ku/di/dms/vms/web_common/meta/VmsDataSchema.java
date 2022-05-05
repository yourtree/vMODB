package dk.ku.di.dms.vms.web_common.meta;

import dk.ku.di.dms.vms.modb.common.meta.ConstraintReference;
import dk.ku.di.dms.vms.modb.common.meta.DataType;
import dk.ku.di.dms.vms.modb.common.meta.ForeignKeyReference;

/**
 * The <code>VmsSchema</code> record describes the schema of VmsTable.
 */
public class VmsDataSchema {

    public String virtualMicroservice;

    public String tableName;

    // identification of columns that form the primary key. all tables must have a primary key
    public int[] primaryKeyColumns;

    // the name of the columns
    public String[] columnNames;

    // the data types of the columns
    public DataType[] columnDataTypes;

    // foreign key references
    public ForeignKeyReference[] foreignKeyReferences; // this can be outside, other vms

    // constraints, referred by column position
    public ConstraintReference[] constraintReferences;

    public VmsDataSchema(String virtualMicroservice, String tableName, int[] primaryKeyColumns, String[] columnNames, DataType[] columnDataTypes, ForeignKeyReference[] foreignKeyReferences, ConstraintReference[] constraintReferences) {
        this.virtualMicroservice = virtualMicroservice;
        this.tableName = tableName;
        this.primaryKeyColumns = primaryKeyColumns;
        this.columnNames = columnNames;
        this.columnDataTypes = columnDataTypes;
        this.foreignKeyReferences = foreignKeyReferences;
        this.constraintReferences = constraintReferences;
    }

}