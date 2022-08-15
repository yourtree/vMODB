package dk.ku.di.dms.vms.modb.storage;

import dk.ku.di.dms.vms.modb.common.meta.DataType;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.util.function.Function;

import static dk.ku.di.dms.vms.modb.common.meta.Constants.DEFAULT_MAX_SIZE_CHAR;

public class DataTypeUtils {

    private DataTypeUtils(){}

    private static final Unsafe UNSAFE = MemoryUtils.UNSAFE;

    public static Object getValue(DataType dt, long address){

        switch (dt) {
            case BYTE -> {
                return UNSAFE.getByte(address);
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

    public static Function<ByteBuffer,?> getFunction(DataType dt){
        switch (dt){
            case BYTE -> {
                return ByteBuffer::get;
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

    private static Object getChar(ByteBuffer buffer) {
        return buffer.get(buffer.array(), buffer.position(), buffer.position() + DEFAULT_MAX_SIZE_CHAR);
    }

}
