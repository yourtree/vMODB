package dk.ku.di.dms.vms.modb.definition.key;

import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.composite.NCompositeKey;
import dk.ku.di.dms.vms.modb.definition.key.composite.PairCompositeKey;
import dk.ku.di.dms.vms.modb.definition.key.composite.QuadrupleCompositeKey;
import dk.ku.di.dms.vms.modb.definition.key.composite.TripleCompositeKey;
import dk.ku.di.dms.vms.modb.index.IIndexKey;

import java.util.Arrays;

public final class KeyUtils {

    private KeyUtils(){}

    public static IKey buildRecordKey(int[] columns, Object[] object){
        switch (columns.length){
            case 1 -> {
                return SimpleKey.of( object[columns[0]] );
            }
            case 2 -> {
                return PairCompositeKey.of(object[columns[0]], object[columns[1]]);
            }
            case 3 -> {
                return TripleCompositeKey.of(object[columns[0]], object[columns[1]], object[columns[2]]);
            }
            case 4 -> {
                return QuadrupleCompositeKey.of(object[columns[0]], object[columns[1]], object[columns[2]], object[columns[3]]);
            }
            default -> {
                if(columns.length == object.length){
                    return NCompositeKey.of( object );
                }
                Object[] values = new Object[columns.length];
                for(int i = 0; i < columns.length; i++){
                    values[i] = object[columns[i]];
                }
                return NCompositeKey.of( values );
            }
        }
    }

    /**
     * Build a key based on the columns
     * @param schema schema
     * @param columns the columns
     * @param srcAddress the src address
     * @return record key
     */
    public static IKey buildRecordKey(Schema schema, int[] columns, long srcAddress){
        IKey key;
        // 2 - build the pk
        if(columns.length == 1){
            DataType columnType = schema.columnDataType( columns[0] );
            srcAddress += schema.columnOffset()[columns[0]];
            key = SimpleKey.of( DataTypeUtils.getValue(columnType, srcAddress) );
        } else {
            Object[] values = new Object[columns.length];
            long currAddress = srcAddress;
            for(int i = 0; i < columns.length; i++){
                DataType columnType = schema.columnDataType( columns[i] );
                currAddress += schema.columnOffset()[columns[i]];
                values[i] = DataTypeUtils.getValue(columnType, currAddress);
                // make it default to get the correct offset next iteration
                currAddress = srcAddress;
            }
            key = NCompositeKey.of( values );
        }
        return key;
    }

    public static IKey buildIndexKey(Object[] values){
        if(values.length == 1) return SimpleKey.of(values[0]);
        return NCompositeKey.of(values);
    }

    public static IIndexKey buildIndexKey(int[] values){
        if(values.length == 1) return SimpleKey.of(values[0]);
        return NCompositeKey.of(Arrays.stream(values).boxed().toArray(Integer[]::new));
    }

}
