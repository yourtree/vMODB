package dk.ku.di.dms.vms.modb.common.type;

import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.function.Function;

import static dk.ku.di.dms.vms.modb.common.type.Constants.DEFAULT_MAX_SIZE_STRING;

public final class DataTypeUtils {

    private DataTypeUtils(){}

    private static final jdk.internal.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;

    public static Object getValue(DataType dt, long address){

        switch (dt) {
            case BOOL -> {
                return UNSAFE.getBoolean(null, address);
            }
            case INT -> {
                return UNSAFE.getInt(null, address);
            }
            case CHAR -> {
                return UNSAFE.getChar(null, address);
            }
            case STRING -> {
                char[] res = new char[DEFAULT_MAX_SIZE_STRING];
                long currAddress = address;
                for(int i = 0; i < DEFAULT_MAX_SIZE_STRING; i++) {
                    res[i] = UNSAFE.getChar(null, currAddress);
                    currAddress += Character.BYTES;
                }
                return res;
            }
            case LONG, DATE -> {
                return UNSAFE.getLong(null, address);
            }
            case FLOAT -> {
                return UNSAFE.getFloat(null, address);
            }
            case DOUBLE -> {
                return UNSAFE.getDouble(null, address);
            }
            default -> throw new IllegalStateException("Unknown data type");
        }

    }

    /**
     * Used by the append-only buffer?
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
                return ByteBuffer::getChar;
            }
             case STRING -> {
                     return DataTypeUtils::getString;
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

    public static Object callReadFunction(long address, DataType dt){
        switch (dt){
            case BOOL -> { return UNSAFE.getBoolean(null, address); }
            case INT -> { return UNSAFE.getInt(null, address); }
            case CHAR -> { return UNSAFE.getChar(null, address); }
            case STRING -> {
                return "";
            }
            case LONG -> { return UNSAFE.getLong(null, address); }
            case DATE -> { return new Date(UNSAFE.getLong(null, address)); }
            case FLOAT -> { return UNSAFE.getFloat(null, address); }
            case DOUBLE -> { return UNSAFE.getDouble(null, address); }
            default -> throw new IllegalStateException("Unknown data type");
        }
    }

    // just a wrapper
    public static void callWriteFunction(long address, DataType dt, Object value){
        switch (dt){
            case BOOL -> // byte is used. on unsafe, the boolean is used
                    UNSAFE.putBoolean(null, address, (boolean)value);
            case INT -> UNSAFE.putInt(null, address, (int)value);
            case CHAR -> UNSAFE.putChar(null, address, (char)value);
            case STRING -> {
                long currPos = address;
                if(value instanceof Character[] charArray) {
                    for (int i = 0; i < DEFAULT_MAX_SIZE_STRING; i++) {
                        UNSAFE.putChar(null, currPos, charArray[i]);
                        currPos += Character.BYTES;
                    }
                } else if (value instanceof String strValue){
                    for (int i = 0; i < DEFAULT_MAX_SIZE_STRING; i++) {
                        UNSAFE.putChar(null, currPos, strValue.charAt(i));
                        currPos += Character.BYTES;
                    }
                }
            }
            case LONG -> UNSAFE.putLong(null, address, (long)value);
            case DATE -> {
                if(value instanceof Date date){
                    UNSAFE.putLong(null, address, date.getTime());
                }
                else UNSAFE.putLong(null, address, (long)value);
            }
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
            case CHAR -> buffer.putChar( (char)value);
            case STRING -> {

                int start = buffer.position();
                if(value instanceof Character[] charArray) {
                    for (int i = 0; i < DEFAULT_MAX_SIZE_STRING && i < charArray.length; i++) {
                        buffer.putChar(charArray[i]);
                    }
                } else if (value instanceof String strValue){

                    for (int i = 0; i < DEFAULT_MAX_SIZE_STRING && i < strValue.length(); i++) {
                        buffer.putChar(strValue.charAt(i));
                    }
                }

                buffer.position(start + (Character.BYTES * DEFAULT_MAX_SIZE_STRING));

            }
            case LONG, DATE -> buffer.putLong((long)value);
            case FLOAT -> buffer.putFloat( (float)value);
            case DOUBLE -> buffer.putDouble((double)value);
            default -> throw new IllegalStateException("Unknown data type");
        }
    }

    private static String getString(ByteBuffer buffer) {
        if(buffer.isDirect()){
            StringBuilder sb = new StringBuilder();
            long currAddress = MemoryUtils.getByteBufferAddress(buffer);
            for(int i = 0; i < DEFAULT_MAX_SIZE_STRING; i++) {
                sb.append( UNSAFE.getChar(currAddress) );
                currAddress += Character.BYTES;
            }
            return sb.toString();
        }
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < DEFAULT_MAX_SIZE_STRING; i++) {
            sb.append( buffer.getChar() );
        }
        return sb.toString();
    }

    public static DataType getDataTypeFromJavaType(Class<?> type){
        if (int.class.equals(type)) {
            return DataType.INT;
        }
        if (long.class.equals(type)) {
            return DataType.LONG;
        }
        if (Date.class.equals(type)) {
            return DataType.DATE;
        }
        if (float.class.equals(type)) {
            return DataType.FLOAT;
        }
        if (double.class.equals(type)) {
            return DataType.DOUBLE;
        }
        if (String.class.equals(type)) {
            return DataType.STRING;
        }
        if (char.class.equals(type)) {
            return DataType.CHAR;
        }
        if (boolean.class.equals(type)) {
            return DataType.BOOL;
        }
        return null;
    }

    public static Class<?> getJavaTypeFromDataType(DataType dataType) {
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
            case STRING -> {
                return String.class;
            }
            case LONG -> {
                return long.class;
            }
            case FLOAT -> {
                return float.class;
            }
            case DOUBLE -> {
                return double.class;
            }
            case DATE -> {
                return Date.class;
            }
            default -> throw new IllegalStateException(dataType + " is not supported.");
        }

    }

}
