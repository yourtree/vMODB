package dk.ku.di.dms.vms.modb.schema.key;

import dk.ku.di.dms.vms.modb.common.meta.DataType;
import dk.ku.di.dms.vms.modb.common.meta.DataTypeUtils;
import dk.ku.di.dms.vms.modb.schema.Schema;

import java.nio.ByteBuffer;

public class KeyUtils {

    private KeyUtils(){}

    public static IKey buildRecordKey(Schema schema, ByteBuffer record, int[] pkColumns){

        IKey key;

        // 2 - build the pk
        if(pkColumns.length == 1){
            DataType columnType = schema.getColumnDataType( pkColumns[0] );
            key = new SimpleKey( DataTypeUtils.getFunction(columnType).apply(record) );
        } else {

            Object[] values = new Object[pkColumns.length];

            for(int i = 0; i < pkColumns.length; i++){
                DataType columnType = schema.getColumnDataType( pkColumns[i] );
                values[i] = DataTypeUtils.getFunction(columnType).apply(record);
            }

            key = new CompositeKey( values );

        }

        return key;
    }

}
