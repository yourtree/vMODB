package dk.ku.di.dms.vms.web_common;

public class WebSocketConstants {

    public static int EVENT_PAYLOAD_SIZE = 256;

    // do this first using the websocket, pay the price of serialization/des
    // and later think about optimization
    //stream processing systems also use serdes
    public static int DATA_PAYLOAD_SIZE = 1024;

    public static class OpCode {

        public static int OPCODE_NONE = 0;

        // HEARTBEAT
        // CONFIGURATION

        public static int OPCODE_TEXT = 1;

        public static int OPCODE_BINARY = 2;

        public static int OPCODE_CLOSE = 8;

        public static int OPCODE_PING = 9;

        public static int OPCODE_PONG = 10;

    }

}
