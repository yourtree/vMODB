package dk.ku.di.dms.vms.sdk.embed.entity;

import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Schema;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public final class EntityUtils {

    private EntityUtils(){}

    private static final MethodHandles.Lookup lookup;
    static {
        lookup = MethodHandles.lookup();
    }

    public static Object[] readEntityFromBuffer(ByteBuffer buffer, Schema schema){

        return null;

    }

    public static Map<String, VarHandle> getFieldsFromEntity(Class<? extends IEntity<?>> entityClazz, Schema schema) throws NoSuchFieldException, IllegalAccessException {

        MethodHandles.Lookup lookup_ = MethodHandles.privateLookupIn(entityClazz, lookup);

        Map<String, VarHandle> fieldMap = new HashMap<>(schema.getColumnNames().length);
        int i = 0;
        for(String columnName : schema.getColumnNames()){
            fieldMap.put(
                    columnName,
                    lookup_.findVarHandle(
                            entityClazz,
                            columnName,
                            DataTypeUtils.getJavaTypeFromDataType(schema.getColumnDataType(i))
                    )
            );
            i++;
        }

        return fieldMap;

    }

}
