package dk.ku.di.dms.vms.sidecar.server;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import static dk.ku.di.dms.vms.sidecar.NetworkUtils.decode;
import static java.util.logging.Logger.getLogger;

/**
 * TODO in the future, if there is no new events, this thread can be put to sleep
 *          and be woken up when new events arrive
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
        byte[] b = new byte[1024];
        InputStream in;
        int len;

        // while not closed connection, continue
        while(!connectionClosed){

            try {

                in = clientSocket.getInputStream();
                len = in.read(b);

                // String data = new Scanner(in,"UTF-8").useDelimiter("\\r\\n\\r\\n").next();

                // logger.info("scanned data: "+data);

                // https://stackoverflow.com/questions/18368130/how-to-parse-and-validate-a-websocket-frame-in-java

                // https://github.com/FirebaseExtended/TubeSock/blob/master/src/main/java/com/firebase/tubesock/WebSocketReceiver.java

                // if close
                if( 8 == (byte) (b[0] & 0xf) ){

                    logger.info("Client disconnected.");

                    connectionClosed = true;
                    this.vmsServer.deRegisterClient(this.clientSocket);
                    continue;
                }

                String message = decode(len, b);

                logger.info("Message received from client: "+message);

                // TODO communicate message to coordinator

                // TODO detect close message

                // len is -1, so no need to allocate a new buffer for the next read
                if (message != null) {
                    b = new byte[1024];
                }

            } catch (IOException e) {
                logger.log(Level.WARNING, e.getMessage());
            }

        }

    }


}
