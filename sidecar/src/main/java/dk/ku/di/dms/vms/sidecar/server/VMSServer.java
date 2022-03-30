package dk.ku.di.dms.vms.sidecar.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.logging.Logger.getLogger;

/**
 * Sources of inspiration:
 * https://crunchify.com/java-nio-non-blocking-io-with-server-client-example-java-nio-bytebuffer-and-channels-selector-java-nio-vs-io/
 * https://www.eclipse.org/jetty/documentation/jetty-11/programming-guide/index.html#advanced-embedding
 * https://codingcraftsman.wordpress.com/2015/07/27/signalling-between-threads-in-java/
 * https://stackoverflow.com/questions/43163592/standalone-websocket-server-without-jee-application-server
 * https://stackoverflow.com/questions/8125507/how-can-i-send-and-receive-websocket-messages-on-the-server-side
 * https://stackoverflow.com/questions/12875418/client-server-connection-using-a-thread-pool
 * http://tutorials.jenkov.com/java-multithreaded-servers/thread-pooled-server.html
 *
 * TODO: think about mechanisms to avoid this thread consuming a CPU forever.
 *       if we just have one client (the runtime), as long as it connected, this thread can be put to sleep
 *       and awake only when a new websocket client connection is made (e.g., the runtime fails)
 *
 * TODO: standardize log messages
 *
 */
public class VMSServer implements Runnable {

    private final Logger logger = getLogger(VMSServer.class.getName());

    private ExecutorService clientHandlerPool;

    private ServerSocket serverSocket;

    private CountDownLatch stopSignalLatch;

    private Map<SocketAddress, Socket> connectedClients;

    private Future<Socket> socketClientFuture;

    public VMSServer() {
        this.clientHandlerPool = Executors.newFixedThreadPool(3);
        this.stopSignalLatch = new CountDownLatch(1);
        this.connectedClients = new ConcurrentHashMap<>();
    }

    public VMSServer(int numberOfWorkers) {
        this.clientHandlerPool = Executors.newFixedThreadPool(numberOfWorkers);
        this.stopSignalLatch = new CountDownLatch(1);
        this.connectedClients = new ConcurrentHashMap<>();
    }

    private void createServerSocket() {
        try {
            this.serverSocket = new ServerSocket(80);
            // TODO make log async?
            // TODO integrate memory handler into modb? https://docs.oracle.com/javase/10/core/java-logging-overview.htm
            logger.info("WebSocket Server has started on 127.0.0.1:80.\r\nWaiting for a connection...");
        } catch (IOException e) {
            throw new RuntimeException("Cannot open port 80", e);
        }
    }

    private boolean isStopped() {
        return this.stopSignalLatch.getCount() == 0;
    }

    public void deRegisterClient(Socket clientSocket){
        this.connectedClients.remove( clientSocket.getLocalSocketAddress() );
    }

    private class SocketClientSupplier implements Supplier<Socket> {

        private final ServerSocket serverSocket;

        public SocketClientSupplier(ServerSocket serverSocket){
            this.serverSocket = serverSocket;
        }

        @Override
        public Socket get() {
            try {
                return serverSocket.accept();
            } catch (IOException e){
                if(serverSocket.isClosed()){
                    logger.info("WebSocket Server is closed.");
                }
                throw new RuntimeException("Error accepting client connection", e);
            }
        }
    }

    private class HandshakeHandler implements Function<Socket, Socket> {

        @Override
        public Socket apply(Socket clientSocket) {

            try {
                logger.info("A client connected. Starting handshake.");

                InputStream in = clientSocket.getInputStream();
                OutputStream out = clientSocket.getOutputStream();

                if (doHandshake(in, out)) {
                    logger.info("Handshake finalized. Client ready for full-duplex communication.");
                    return clientSocket;
                } else {
                    clientSocket.close();
                    logger.info("Handshake failed.");
                    return null;
                }
            } catch (IOException e) {
                if (serverSocket.isClosed()) {
                    logger.info("WebSocket Server is closed.");
                }
                throw new RuntimeException("Error accepting client connection", e);
            } catch (NoSuchAlgorithmException e){
                logger.info("Error encoding handshake message");
                throw new RuntimeException("Error encoding handshake message", e);
            }

        }

        private boolean doHandshake(InputStream inputStream, OutputStream outputStream)
                throws IOException, NoSuchAlgorithmException {

            String data = new Scanner(inputStream,"UTF-8").useDelimiter("\\r\\n\\r\\n").next();
            Matcher get = Pattern.compile("^GET").matcher(data);

            if (get.find()) {
                Matcher match = Pattern.compile("Sec-WebSocket-Key: (.*)").matcher(data);
                match.find();

                byte[] response = ("HTTP/1.1 101 Switching Protocols\r\n"
                        + "Connection: Upgrade\r\n"
                        + "Upgrade: websocket\r\n"
                        + "Sec-WebSocket-Accept: "
                        + Base64.getEncoder().
                        encodeToString(MessageDigest.getInstance("SHA-1")
                                .digest((match.group(1) + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
                                        .getBytes("UTF-8")))
                        + "\r\n\r\n").getBytes("UTF-8");

                outputStream.write(response, 0, response.length);
                outputStream.flush();

                return true;
            } else {
                return false;
            }

        }

    }

    public void run() {

        // run does not allow for throwing exceptions, so we encapsulate the exception in a method
        createServerSocket();

        while( !isStopped() ){

            handleWebSocketClients();

            // TODO how to push data to the websocket clients????

        }

        this.clientHandlerPool.shutdown();
        logger.info("VMSServer Server stopped.");

    }

    private void handleWebSocketClients() {

        // not blocking the vms server
        // TODO do we need a particular executor for this?
        //      yes if we want to have more control over the number of available workers for clients
        if(this.socketClientFuture == null){
            // https://stackoverflow.com/questions/43019126/completablefuture-thenapply-vs-thencompose
            this.socketClientFuture = CompletableFuture.
                    supplyAsync( new SocketClientSupplier(this.serverSocket) )
                    .thenApplyAsync(
                            // I am not returning a completable future, so use apply instead of compose
                            socket -> new HandshakeHandler().apply(socket)
                    );
        } else if( socketClientFuture.isDone() ){
            try {
                Socket clientSocket = socketClientFuture.get();
                // handshake failed
                if(clientSocket == null) {
                    this.socketClientFuture = null;
                    return;
                }
                // store socket
                connectedClients.put(clientSocket.getLocalSocketAddress(), clientSocket);

                logger.info("Creating new client handler.");

                clientHandlerPool.submit(new WebSocketClientHandler(this, clientSocket));
                this.socketClientFuture = null;

            } catch (ExecutionException | InterruptedException e) {
                this.socketClientFuture = null;
                logger.info("Error obtaining future: "+e.getLocalizedMessage());
            }
        }
    }

    public void stop(){
        this.stopSignalLatch.countDown();
        try {
            synchronized(this) {
                if(!serverSocket.isClosed()) {
                    this.serverSocket.close();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error closing server", e);
        }
    }



}
