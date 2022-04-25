package dk.ku.di.dms.vms.sidecar.server;

import dk.ku.di.dms.vms.web_common.utils.ByteUtils;

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
public class AsyncSocketEventHandler implements Runnable {

    private final Logger logger = getLogger(AsyncSocketEventHandler.class.getName());

    private final AsyncVMSServer vmsServer;

    private final AsynchronousSocketChannel clientSocket;

    private boolean connectionClosed;

    private ByteBuffer buffer;

    public AsyncSocketEventHandler(AsyncVMSServer vmsServer, AsynchronousSocketChannel clientSocket) {
        this.vmsServer = vmsServer;
        this.clientSocket = clientSocket;
        this.connectionClosed = false;

        // TODO revisit this size later
        // The idea is to have an initial large enough buffer to accommodate
        // the majority of incoming request payload sizes
        this.buffer = ByteBuffer.allocateDirect( 1024 );
    }

    @Override
    public void run() {

        logger.info("Running new client handler...");

        Future<Integer> in;
        int len = 0;

        // while not closed connection, continue
        while(!connectionClosed && clientSocket.isOpen()){

            try {
                in = clientSocket.read(buffer);
                len = in.get();

                if(len == -1){
                    // not possible to read the buffer...
                    logger.log(Level.WARNING, "ERR: something went wrong on reading the buffer");
                    buffer.flip();
                    continue;
                }



                /*

                // compare the number of bytes read with the actual size of the object
                ByteBuffer slice = buffer.slice(0,4);
                int size = ByteUtils.fromByteArrayToInteger( slice.array() );

                // completed!!
                if(size == len){

                    String message = new String( buffer.array(), 0, len );
                    in.get();
                    buffer.flip();

                    // String message = new Scanner(buffer,"UTF-8").useDelimiter("\\r\\n\\r\\n").next();
                    logger.info(message);


                } else {

                    int totalRead = len;
                    boolean error = false;

                    // read until the buffer in completed
                    do {



                    } while ( totalRead < size && !error );


                }

                */



                // TODO communicate message to coordinator

            } catch (ExecutionException | InterruptedException e) {
                logger.log(Level.WARNING, e.getMessage());
            }

        }

    }

}
