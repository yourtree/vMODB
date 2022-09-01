package dk.ku.di.dms.vms.modb.common.meta;

import sun.misc.Unsafe;

import static dk.ku.di.dms.vms.modb.common.meta.Constants.DEFAULT_MAX_SIZE_CHAR;

public class DataTypeUtils {

    private DataTypeUtils(){}

    private static final Unsafe UNSAFE = MemoryUtils.UNSAFE;

    public static Object getValue(DataType dt, long address){

        switch (dt) {
            case BOOL -> {
                return UNSAFE.getBoolean(null, address);
            }
            case INT -> {
                return UNSAFE.getInt(address);
            }
            case CHAR -> {
                char[] res = new char[DEFAULT_MAX_SIZE_CHAR];
                long startAddress = address;
                for(int i = 0; i < DEFAULT_MAX_SIZE_CHAR; i++) {
                    res[i] = UNSAFE.getChar(startAddress);
                    startAddress += Character.BYTES;
                }
                return res;
            }
            case LONG, DATE -> {
                return UNSAFE.getLong(address);
            }
            case FLOAT -> {
                return UNSAFE.getFloat(address);
            }
            case DOUBLE -> {
                return UNSAFE.getDouble(address);
            }
            default -> throw new IllegalStateException("Unknown data type");

        }

    }

}
