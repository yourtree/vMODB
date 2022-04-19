package dk.ku.di.dms.vms.sidecar.server;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.logging.Logger.getLogger;

/**
 * TODO in the future, if there is no new events, this thread can be put to sleep
 *          and be woken up when new events arrive
 *          Sources:
 *          https://stackoverflow.com/questions/18368130/how-to-parse-and-validate-a-websocket-frame-in-java
 *          https://github.com/FirebaseExtended/TubeSock/blob/master/src/main/java/com/firebase/tubesock/WebSocketReceiver.java
 *          https://blog.sessionstack.com/how-javascript-works-deep-dive-into-websockets-and-http-2-with-sse-how-to-pick-the-right-path-584e6b8e3bf7
 */
public class AsyncSocketClientHandler implements Runnable {

    private final Logger logger = getLogger(AsyncSocketClientHandler.class.getName());

    private final AsyncVMSServer vmsServer;

    private final AsynchronousSocketChannel clientSocket;

    private boolean connectionClosed;

    public AsyncSocketClientHandler(AsyncVMSServer vmsServer, AsynchronousSocketChannel clientSocket) {
        this.vmsServer = vmsServer;
        this.clientSocket = clientSocket;
        this.connectionClosed = false;
    }

    @Override
    public void run() {

        logger.info("Running new client handler...");

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        Future<Integer> in;
        int len = 0;

        // while not closed connection, continue
        while(!connectionClosed && clientSocket.isOpen()){

            try {
                in = clientSocket.read(buffer);
                len = in.get();

                String message = new String( buffer.array(), 0, len );
                in.get();
                buffer.flip();

                // String message = new Scanner(buffer,"UTF-8").useDelimiter("\\r\\n\\r\\n").next();
                logger.info(message);

            } catch (ExecutionException | InterruptedException e) {
                logger.log(Level.WARNING, e.getMessage());
            }

            /*

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



                // TODO communicate message to coordinator

                continue;
            }

            // if payload is string, used exchanging events and (data model) entities
            if(opcode == OpCode.OPCODE_TEXT){

                String message = null;
                try {
                    message = WebSocketUtils.StringOperations.decode(len, b);

                    // len is -1, so no need to allocate a new buffer for the next read
                    if (message != null) {
                        logger.info("Message received from client: "+message);
                        b = new byte[EVENT_PAYLOAD_SIZE];
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }



            }
            else
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

             */

        }

    }

}
