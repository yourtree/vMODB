package dk.ku.di.dms.vms.modb.schema.key;

import dk.ku.di.dms.vms.modb.common.meta.DataType;
import dk.ku.di.dms.vms.modb.storage.DataTypeUtils;
import dk.ku.di.dms.vms.modb.schema.Schema;
import dk.ku.di.dms.vms.modb.storage.MemoryUtils;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;

public class KeyUtils {

    private KeyUtils(){}

    public static IKey buildRecordKey(Schema schema, int[] columns, long recordAddress){

        IKey key;

        // 2 - build the pk
        if(columns.length == 1){
            DataType columnType = schema.getColumnDataType( columns[0] );
            recordAddress += schema.columnOffset()[columns[0]];
            key = new SimpleKey( DataTypeUtils.getValue(columnType, recordAddress) );
        } else {

            Object[] values = new Object[columns.length];
            long currAddress = recordAddress;

            for(int i = 0; i < columns.length; i++){
                DataType columnType = schema.getColumnDataType( columns[i] );
                currAddress += schema.columnOffset()[columns[i]];
                values[i] = DataTypeUtils.getValue(columnType, currAddress);
                currAddress = recordAddress;
            }

            key = new CompositeKey( values );

        }

        return key;
    }

    public static IKey buildRecordKey(Schema schema, ByteBuffer record, int[] columns){

        IKey key;

        // 2 - build the pk
        if(columns.length == 1){
            DataType columnType = schema.getColumnDataType( columns[0] );
            record.position( schema.columnOffset()[columns[0]] );
            key = new SimpleKey( DataTypeUtils.getFunction(columnType).apply(record) );
        } else {

            Object[] values = new Object[columns.length];

            for(int i = 0; i < columns.length; i++){
                DataType columnType = schema.getColumnDataType( columns[i] );
                record.position( schema.columnOffset()[columns[i]] );
                values[i] = DataTypeUtils.getFunction(columnType).apply(record);
            }

            key = new CompositeKey( values );

        }

        record.position(0);
        return key;

    }

}
