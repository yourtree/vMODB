package dk.ku.di.dms.vms.modb.common.utils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ByteUtils {

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
            buffer.position(buffer.position() + size);
        }
        return extracted;
    }

}
