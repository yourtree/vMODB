package dk.ku.di.dms.vms.modb.definition.key;

import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;

public class KeyUtils {

    private KeyUtils(){}

    public static IKey buildRecordKey(Object... object){
        if(object.length == 1){
            return SimpleKey.of( object[0] );
        }
        return CompositeKey.of( object );
    }

    public static IKey buildRecordKey(Schema schema, int[] columns, Object... object){

        if(columns.length == 1){
            return SimpleKey.of( object[0] );
        }

        if(columns.length == object.length){
            return CompositeKey.of( object );
        }

        Object[] values = new Object[columns.length];
        int offset;
        for(int i = 0; i < columns.length; i++){
            offset = schema.getColumnOffset(columns[i]);
            values[i] = object[offset];
        }

        return CompositeKey.of( values );

    }

    /**
     * Build a key based on the columns
     * @param schema schema
     * @param columns the columns
     * @param srcAddress the src address
     * @return
     */
    public static IKey buildRecordKey(Schema schema, int[] columns, long srcAddress){

        IKey key;

        // 2 - build the pk
        if(columns.length == 1){
            DataType columnType = schema.getColumnDataType( columns[0] );
            srcAddress += schema.columnOffset()[columns[0]];
            key = SimpleKey.of( DataTypeUtils.getValue(columnType, srcAddress) );
        } else {

            Object[] values = new Object[columns.length];
            long currAddress = srcAddress;

            for(int i = 0; i < columns.length; i++){
                DataType columnType = schema.getColumnDataType( columns[i] );
                currAddress += schema.columnOffset()[columns[i]];
                values[i] = DataTypeUtils.getValue(columnType, currAddress);
                currAddress = srcAddress;
            }

            key = CompositeKey.of( values );

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
