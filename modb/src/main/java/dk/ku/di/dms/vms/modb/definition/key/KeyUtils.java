package dk.ku.di.dms.vms.modb.definition.key;

import dk.ku.di.dms.vms.modb.common.meta.DataType;
import dk.ku.di.dms.vms.modb.common.meta.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Schema;

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

    public static IKey buildInputKey(Object[] values){
        if(values.length == 1){
            return SimpleKey.of(values[0]);
        }
        return CompositeKey.of(values);
    }

}
