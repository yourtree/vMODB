package dk.ku.di.dms.vms.sidecar.server;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
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

                String message = decode(len, b);

                // TODO communicate message to coordinator

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
