package dk.ku.di.dms.vms.sdk.embed.entity;

import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataSchema;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public final class EntityUtils {

    private EntityUtils(){}

    private static final MethodHandles.Lookup lookup;
    static {
        lookup = MethodHandles.lookup();
    }

    public static Map<String, VarHandle> getFieldsFromCompositePk(Class<?> pkClazz) throws NoSuchFieldException, IllegalAccessException {

        MethodHandles.Lookup lookup_ = MethodHandles.privateLookupIn(pkClazz, lookup);

        Field[] fields = pkClazz.getDeclaredFields();
        Map<String, VarHandle> fieldMap = new HashMap<>(  );

        for(Field field : fields){
            fieldMap.put(
                    field.getName(),
                    lookup_.findVarHandle(
                            pkClazz,
                            field.getName(),
                            field.getType()
                    )
            );
        }
        return fieldMap;
    }

    public static Map<String, VarHandle> getFieldFromPk(Class<?> parentClazz, Class<?> pkClazz, VmsDataSchema dataSchema) throws NoSuchFieldException, IllegalAccessException {

        // usually the first, but to make sure lets do like this
        int pkColumn = dataSchema.primaryKeyColumns[0];

        String pkColumnName = dataSchema.columnNames[pkColumn];

        Map<String, VarHandle> fieldMap = new HashMap<>(1);
        fieldMap.put(
                pkColumnName,
                lookup.findVarHandle(
                        parentClazz,
                        pkColumnName,
                        parentClazz.getDeclaredField(pkColumnName).getType()
                )
        );
        return fieldMap;
    }

    public static Map<String, VarHandle> getFieldsFromEntity(Class<? extends IEntity<?>> entityClazz,
                                                             VmsDataSchema schema) throws NoSuchFieldException, IllegalAccessException {

        MethodHandles.Lookup lookup_ = MethodHandles.privateLookupIn(entityClazz, lookup);

        Map<String, VarHandle> fieldMap = new HashMap<>(schema.columnNames.length);
        int i = 0;
        for(String columnName : schema.columnNames){
            fieldMap.put(
                    columnName,
                    lookup_.findVarHandle(
                            entityClazz,
                            columnName,
                            DataTypeUtils.getJavaTypeFromDataType(schema.columnDataTypes[i])
                    )
            );
            i++;
        }

        return fieldMap;

    }

}
