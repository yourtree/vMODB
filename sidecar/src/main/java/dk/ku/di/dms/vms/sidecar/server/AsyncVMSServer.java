package dk.ku.di.dms.vms.sidecar.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.function.Function;
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
 *  This may be the answer: https://liakh-aliaksandr.medium.com/java-sockets-i-o-blocking-non-blocking-and-asynchronous-fb7f066e4ede
 *
 * TODO: standardize log messages
 *
 */
public class AsyncVMSServer implements Runnable {

    private final Logger logger = getLogger(AsyncVMSServer.class.getName());

    private final ExecutorService clientHandlerPool;

    // private ServerSocket serverSocket;
    private AsynchronousServerSocketChannel serverSocket;

    private final CountDownLatch stopSignalLatch;

    private final Map<SocketAddress, AsynchronousSocketChannel> connectedClients;

    private Future<AsynchronousSocketChannel> socketClientFuture;

    public AsyncVMSServer() {
        this.clientHandlerPool = Executors.newFixedThreadPool(3);
        this.stopSignalLatch = new CountDownLatch(1);
        this.connectedClients = new ConcurrentHashMap<SocketAddress, AsynchronousSocketChannel>();
    }

    public AsyncVMSServer(int numberOfWorkers) {
        this.clientHandlerPool = Executors.newFixedThreadPool(numberOfWorkers);
        this.stopSignalLatch = new CountDownLatch(1);
        this.connectedClients = new ConcurrentHashMap<SocketAddress, AsynchronousSocketChannel>();
    }

    private void createServerSocket() {
        try {
            // this.serverSocket = new ServerSocket(80);

            this.serverSocket = AsynchronousServerSocketChannel.open();
            this.serverSocket.bind(new InetSocketAddress(80));

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
        this.connectedClients.remove( clientSocket.getRemoteSocketAddress() );
    }

    private class HandshakeHandler implements Function<AsynchronousSocketChannel, AsynchronousSocketChannel> {

        @Override
        public AsynchronousSocketChannel apply(AsynchronousSocketChannel socketChannel) {

            try {
                logger.info("A client connected. Starting handshake.");

                if (doHandshake(socketChannel)) {
                    logger.info("Handshake finalized. Client ready for full-duplex communication.");
                    return socketChannel;
                } else {
                    socketChannel.close();
                    logger.info("Handshake failed.");
                    return null;
                }
            } catch (ExecutionException | InterruptedException | IOException e) {
                if (!serverSocket.isOpen()) {
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

            String data = new Scanner(inputStream, "UTF-8").useDelimiter("\\r\\n\\r\\n").next();
            Matcher get = Pattern.compile("^GET").matcher(data);

            if (get.find()) {
                Matcher match = Pattern.compile("Sec-WebSocket-Key: (.*)").matcher(data);
                // match.find();

                byte[] response = ("HTTP/1.1 101 Switching Protocols\r\n"
                        + "Connection: Upgrade\r\n"
                        + "Upgrade: websocket\r\n"
                        + "Sec-WebSocket-Accept: "
                        + Base64.getEncoder().
                        encodeToString(MessageDigest.getInstance("SHA-1")
                                .digest((match.group(1) + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
                                        .getBytes(StandardCharsets.UTF_8)))
                        + "\r\n\r\n").getBytes(StandardCharsets.UTF_8);

                outputStream.write(response, 0, response.length);
                outputStream.flush();

                return true;
            } else {
                return false;
            }
        }

        private boolean doHandshake(AsynchronousSocketChannel socketChannel)
                throws NoSuchAlgorithmException, ExecutionException, InterruptedException {

            ByteBuffer buffer = ByteBuffer.allocate(1024);
            Future<Integer> ft = socketChannel.read(buffer);
            ft.get();
            byte[] hb = buffer.array();
            String data = new String( hb );
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
                                        .getBytes(StandardCharsets.UTF_8)))
                        + "\r\n\r\n").getBytes(StandardCharsets.UTF_8);

                ByteBuffer resp = ByteBuffer.wrap( response );
                Future<Integer> res = socketChannel.write(resp);
                res.get();

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

            // independent channels

            // handler for events  ---  this requires a data frame not so big
            handleSocketClient();

            // TODO handler for data  --- this requires a data frame bigger


            // TODO handle coordinator events
            //  // sidecar could do some local scheduling according to the global schedule recevied from the global scheduler

            // TODO push data to the sdk-runtime ????


        }

        this.clientHandlerPool.shutdown();
        logger.info("VMSServer Server stopped.");

    }

    private void handleSocketClient() {

        // not blocking the vms server
        // TODO do we need a particular executor for this?
        //      yes if we want to have more control over the number of available workers for clients
        if(socketClientFuture == null){
            // https://stackoverflow.com/questions/43019126/completablefuture-thenapply-vs-thencompose

//            socketClientFuture = CompletableFuture
//                    .supplyAsync( new SocketClientSupplier(this.serverSocket) )
//                    .thenApply(
//                        // I am not returning a completable future, so use apply instead of compose
//                        socket -> new HandshakeHandler().apply(socket)
//                    );

            // I don't need to explicitly program an async accept, since the Java API gives me that by design
            socketClientFuture = serverSocket.accept();

        } else if( socketClientFuture.isDone() ){
            try {
                AsynchronousSocketChannel clientSocket = socketClientFuture.get();

                // clientSocket = new HandshakeHandler().apply(clientSocket);

                // handshake failed
                if(clientSocket == null) {
                    socketClientFuture = null;
                    return;
                }

                // store socket
                connectedClients.put(clientSocket.getRemoteAddress(), clientSocket);

                logger.info("Creating new client handler.");

                clientHandlerPool.submit(new AsyncSocketClientHandler(this, clientSocket));
                socketClientFuture = null;

            } catch (ExecutionException | InterruptedException | IOException e) {
                socketClientFuture = null;
                logger.info("Error obtaining future: "+e.getLocalizedMessage());
            }

        }
    }

    public void stop(){
        this.stopSignalLatch.countDown();
        try {
            synchronized(this) {
                if(serverSocket.isOpen()) {
                    this.serverSocket.close();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error closing server", e);
        }
    }



}
