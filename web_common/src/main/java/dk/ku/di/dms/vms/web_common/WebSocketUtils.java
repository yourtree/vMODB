package dk.ku.di.dms.vms.web_common;

import java.io.*;

public class WebSocketUtils {

    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        return out.toByteArray();
    }

    public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return is.readObject();
    }

    public static class BinaryOperations {

        public static byte[] decode(int len, byte[] b){

            byte rLength = 0;
            int rMaskIndex = 2;
            int rDataStart = 0;
            //b[0] is always text in my case so no need to check;
            byte data = b[1];
            byte op = (byte) 127;
            rLength = (byte) (data & op);

            if (rLength == (byte) 126) rMaskIndex = 4;
            if (rLength == (byte) 127) rMaskIndex = 10;

            byte[] masks = new byte[4];

            int j = 0;
            int i = 0;
            for (i = rMaskIndex; i < (rMaskIndex + 4); i++) {
                masks[j] = b[i];
                j++;
            }

            rDataStart = rMaskIndex + 4;

            int messLen = len - rDataStart;

            byte[] message = new byte[messLen];

            for (i = rDataStart, j = 0; i < len; i++, j++) {
                message[j] = (byte) (b[i] ^ masks[j % 4]);
            }

            return message;

        }

    }

    public static class StringOperations {

        public static String decode(int len, byte[] b) throws IOException {
            if (len != -1) {

                byte rLength = 0;
                int rMaskIndex = 2;
                int rDataStart = 0;
                //b[0] is always text in my case so no need to check;
                byte data = b[1];
                byte op = (byte) 127;
                rLength = (byte) (data & op);

                if (rLength == (byte) 126) rMaskIndex = 4;
                if (rLength == (byte) 127) rMaskIndex = 10;

                byte[] masks = new byte[4];

                int j = 0;
                int i = 0;
                for (i = rMaskIndex; i < (rMaskIndex + 4); i++) {
                    masks[j] = b[i];
                    j++;
                }

                rDataStart = rMaskIndex + 4;

                int messLen = len - rDataStart;

                byte[] message = new byte[messLen];

                for (i = rDataStart, j = 0; i < len; i++, j++) {
                    message[j] = (byte) (b[i] ^ masks[j % 4]);
                }

                return new String(message);

            }
            return null;
        }

        public static byte[] encode(String mess) throws IOException {
            byte[] rawData = mess.getBytes();

            int frameCount = 0;
            byte[] frame = new byte[10];

            frame[0] = (byte) 129;

            if (rawData.length <= 125) {
                frame[1] = (byte) rawData.length;
                frameCount = 2;
            } else if (rawData.length <= 65535) {
                frame[1] = (byte) 126;
                int len = rawData.length;
                frame[2] = (byte) ((len >> 8) & (byte) 255);
                frame[3] = (byte) (len & (byte) 255);
                frameCount = 4;
            } else {
                frame[1] = (byte) 127;
                int len = rawData.length;
                frame[2] = (byte) ((len >> 56) & (byte) 255);
                frame[3] = (byte) ((len >> 48) & (byte) 255);
                frame[4] = (byte) ((len >> 40) & (byte) 255);
                frame[5] = (byte) ((len >> 32) & (byte) 255);
                frame[6] = (byte) ((len >> 24) & (byte) 255);
                frame[7] = (byte) ((len >> 16) & (byte) 255);
                frame[8] = (byte) ((len >> 8) & (byte) 255);
                frame[9] = (byte) (len & (byte) 255);
                frameCount = 10;
            }

            int bLength = frameCount + rawData.length;

            byte[] reply = new byte[bLength];

            int bLim = 0;
            for (int i = 0; i < frameCount; i++) {
                reply[bLim] = frame[i];
                bLim++;
            }
            for (int i = 0; i < rawData.length; i++) {
                reply[bLim] = rawData[i];
                bLim++;
            }

            return reply;
        }

    }

}
