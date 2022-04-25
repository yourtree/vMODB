package dk.ku.di.dms.vms.modb.common.meta;

/**
 * The <code>VmsEventSchema</code> record describes the schema of Transactional Events.
 *
 */
public record VmsEventSchema(

    String eventName, // the respective queue name

    // the name of the columns
    String[] columnNames,

    // the data types of the columns
    DataType[] columnDataTypes

){}