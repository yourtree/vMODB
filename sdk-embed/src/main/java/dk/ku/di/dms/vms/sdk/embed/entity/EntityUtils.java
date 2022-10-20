package dk.ku.di.dms.vms.sdk.embed.entity;

import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.common.schema.VmsDataSchema;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Schema;

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

    public static Map<String, VarHandle> getFieldsFromPk(Class<?> pkClazz) throws NoSuchFieldException, IllegalAccessException {

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

    public static VarHandle getPrimitiveFieldOfPk(Class<?> pkClazz, VmsDataSchema dataSchema) throws NoSuchFieldException, IllegalAccessException {

        MethodHandles.Lookup lookup_ = MethodHandles.privateLookupIn(pkClazz, lookup);

        // usually the first, but to make sure lets do like this
        int pkColumn = dataSchema.primaryKeyColumns[0];

        String pkColumnName = dataSchema.columnNames[pkColumn];

        Field[] fields = pkClazz.getDeclaredFields();

        for(Field field : fields){
            if(field.getName().equalsIgnoreCase(pkColumnName)){
                return lookup_.findVarHandle(
                        pkClazz,
                        field.getName(),
                        field.getType()
                );
            }
        }
        throw new IllegalStateException("Cannot find var handle of primitive primary key.");
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
