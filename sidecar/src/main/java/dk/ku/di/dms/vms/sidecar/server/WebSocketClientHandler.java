package dk.ku.di.dms.vms.sidecar.server;

import dk.ku.di.dms.vms.modb.common.event.IEvent;
import dk.ku.di.dms.vms.web_common.WebSocketUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import static dk.ku.di.dms.vms.web_common.WebSocketConstants.EVENT_PAYLOAD_SIZE;
import static dk.ku.di.dms.vms.web_common.WebSocketUtils.deserialize;
import static java.util.logging.Logger.getLogger;
import static dk.ku.di.dms.vms.web_common.WebSocketConstants.OpCode;

/**
 * TODO in the future, if there is no new events, this thread can be put to sleep
 *          and be woken up when new events arrive
 *          Sources:
 *          https://stackoverflow.com/questions/18368130/how-to-parse-and-validate-a-websocket-frame-in-java
 *          https://github.com/FirebaseExtended/TubeSock/blob/master/src/main/java/com/firebase/tubesock/WebSocketReceiver.java
 *          https://blog.sessionstack.com/how-javascript-works-deep-dive-into-websockets-and-http-2-with-sse-how-to-pick-the-right-path-584e6b8e3bf7
 */
public class WebSocketClientHandler implements Runnable {

    private final Logger logger = getLogger(WebSocketClientHandler.class.getName());

    private final VMSServer vmsServer;

    private final Socket clientSocket;

    private boolean connectionClosed;

    public WebSocketClientHandler(VMSServer vmsServer, Socket clientSocket) {
        this.vmsServer = vmsServer;
        this.clientSocket = clientSocket;
        this.connectionClosed = false;
    }

    @Override
    public void run() {

        // TODO perhaps use bytebuffer?
        byte[] b = new byte[EVENT_PAYLOAD_SIZE];
        InputStream in;
        int len;

        // while not closed connection, continue
        while(!connectionClosed){

            try {
                in = clientSocket.getInputStream();
                len = in.read(b);
            } catch (IOException e) {
                logger.log(Level.WARNING, e.getMessage());
                continue;
            }

            int opcode = (byte) (b[0] & 0x0f);

            // if payload binary, used for obtaining virtual microservice metadata
            if(opcode == OpCode.OPCODE_BINARY){

                byte[] data = WebSocketUtils.BinaryOperations.decode(len,b);

                try {
                    IEvent event = (IEvent) deserialize(data);
                    logger.info("The event was successfully parsed.");
                } catch (ClassNotFoundException | IOException e) {
                    e.printStackTrace();
                }

                /*
                String message = decode(len, b);

                // len is -1, so no need to allocate a new buffer for the next read
                if (message != null) {
                    logger.info("Message received from client: "+message);
                    b = new byte[MESSAGE_PAYLOAD_SIZE];
                }
                */

                // TODO communicate message to coordinator

                continue;
            }

            // if payload is string, used exchanging events and (data model) entities
            if(opcode == OpCode.OPCODE_TEXT){

            }

            // if close
            if(opcode == OpCode.OPCODE_CLOSE){
                connectionClosed = true;
                vmsServer.deRegisterClient(this.clientSocket);
                logger.info("Client disconnected.");

                try {
                    clientSocket.getOutputStream().write(b, 0, len);
                    clientSocket.getOutputStream().flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                return;

            }

        }

    }

}
