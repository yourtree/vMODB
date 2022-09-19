package dk.ku.di.dms.vms.modb.common;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ByteUtils {

    /**
     * https://stackoverflow.com/questions/2183240/java-integer-to-byte-array
     */
//    public static final byte[] intToByteArray(int value) {
//        return new byte[] {
//                (byte)(value >>> 24),
//                (byte)(value >>> 16),
//                (byte)(value >>> 8),
//                (byte)value};
//    }

    public static String extractStringFromByteBuffer(ByteBuffer buffer, int size){
        String extracted;
        if(buffer.isDirect()){
            byte[] byteArray = new byte[size];
            for(int i = 0; i < size; i++){
                byteArray[i] = buffer.get();
            }
            extracted = new String(byteArray, 0, size, StandardCharsets.UTF_8);
        } else {
            extracted = new String(buffer.array(), buffer.position(), size, StandardCharsets.UTF_8);
        }
        buffer.position(buffer.position() + size);
        return extracted;
    }

    public static byte[] fromIntegerToByteArray( int data ) {
        byte[] result = new byte[4];
        result[0] = (byte) ((data & 0xFF000000) >> 24);
        result[1] = (byte) ((data & 0x00FF0000) >> 16);
        result[2] = (byte) ((data & 0x0000FF00) >> 8);
        result[3] = (byte) ((data & 0x000000FF));
        return result;
    }

    // https://stackoverflow.com/questions/7619058/convert-a-byte-array-to-integer-in-java-and-vice-versa
    public static int fromByteArrayToInteger( byte[] bytes ) {
        return ((bytes[0] & 0xFF) << 24) |
                ((bytes[1] & 0xFF) << 16) |
                ((bytes[2] & 0xFF) << 8 ) |
                ((bytes[3] & 0xFF));
    }

}
