package dk.ku.di.dms.vms.modb.common.meta;

import java.nio.ByteBuffer;
import java.util.function.Function;

import static dk.ku.di.dms.vms.modb.common.meta.Constants.DEFAULT_MAX_SIZE_CHAR;

public class DataTypeUtils {

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
            default -> {
                throw new IllegalStateException("Unknown data type");
            }
        }
    }

    private static Object getChar(ByteBuffer buffer) {
        return buffer.get(buffer.array(), buffer.position(), buffer.position() + DEFAULT_MAX_SIZE_CHAR);
    }

}
