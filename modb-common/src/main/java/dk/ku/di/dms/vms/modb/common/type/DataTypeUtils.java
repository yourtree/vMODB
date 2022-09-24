package dk.ku.di.dms.vms.modb.common.type;

import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.util.function.Function;

import static dk.ku.di.dms.vms.modb.common.type.Constants.DEFAULT_MAX_SIZE_CHAR;

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
                long currAddress = address;
                for(int i = 0; i < DEFAULT_MAX_SIZE_CHAR; i++) {
                    res[i] = UNSAFE.getChar(currAddress);
                    currAddress += Character.BYTES;
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

    /**
     * Used by the append only buffer?
     * @param dt Data type
     * @return the corresponding function
     */
    public static Function<ByteBuffer,?> getReadFunction(DataType dt){
        switch (dt){
            case BOOL -> {
                 return ByteBuffer::get; // byte is used. on unsafe, the boolean is used
             }
             case INT -> {
                     return ByteBuffer::getInt;
                 }
             case CHAR -> {
                     return DataTypeUtils::getChar;
                 }
             case LONG, DATE -> {
                     return ByteBuffer::getLong;
                 }
             case FLOAT -> {
                     return ByteBuffer::getFloat;
                 }
             case DOUBLE -> {
                     return ByteBuffer::getDouble;
                 }
             default -> throw new IllegalStateException("Unknown data type");
        }
    }

    // just a wrapper
    public static void callWriteFunction(long address, DataType dt, Object value){
        switch (dt){
            case BOOL -> // byte is used. on unsafe, the boolean is used
                    UNSAFE.putByte(null, address, (byte)value);
            case INT -> UNSAFE.putInt(null, address, (int)value);
            case CHAR -> {
                Character[] charArray = (Character[]) value;
                long currPos = address;
                for(int i = 0; i < DEFAULT_MAX_SIZE_CHAR; i++) {
                    UNSAFE.putChar(null, currPos, charArray[i]);
                    currPos += Character.BYTES;
                }
            }
            case LONG, DATE -> UNSAFE.putLong(null, address, (long)value);
            case FLOAT -> UNSAFE.putFloat(null, address, (float)value);
            case DOUBLE -> UNSAFE.putDouble(null, address, (double)value);
            default -> throw new IllegalStateException("Unknown data type");
        }
    }

    public static void callWriteFunction(ByteBuffer buffer, DataType dt, Object value){

        switch (dt){
            case BOOL -> // byte is used. on unsafe, the boolean is used
                    buffer.put( (byte)value);
            case INT -> buffer.putInt( (int)value);
            case CHAR -> {
                Character[] charArray = (Character[]) value;
                for(int i = 0; i < DEFAULT_MAX_SIZE_CHAR; i++) {
                    buffer.putChar(charArray[i]);
                }
            }
            case LONG, DATE -> buffer.putLong((long)value);
            case FLOAT -> buffer.putFloat( (float)value);
            case DOUBLE -> buffer.putDouble((double)value);
            default -> throw new IllegalStateException("Unknown data type");
        }
    }

    private static Object getChar(ByteBuffer buffer) {
        return buffer.get(buffer.array(), buffer.position(), buffer.position() + DEFAULT_MAX_SIZE_CHAR);
    }

    public static Class<?> getTypeFromDataType(DataType dataType) {

        switch (dataType) {
            case BOOL -> {
                return boolean.class;
            }
            case INT -> {
                return int.class;
            }
            case CHAR -> {
                return char.class;
            }
            case LONG, DATE -> {
                return long.class;
            }
            case FLOAT -> {
                return float.class;
            }
            case DOUBLE -> {
                return double.class;
            }
            default -> throw new IllegalStateException(dataType + " is not supported.");
        }

    }

}
