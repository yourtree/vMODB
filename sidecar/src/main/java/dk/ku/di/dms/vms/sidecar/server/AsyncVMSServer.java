package dk.ku.di.dms.vms.sidecar.server;

import java.io.IOException;
import java.net.*;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Logger;

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

    public void run() {

        // run does not allow for throwing exceptions, so we encapsulate the exception in a method
        createServerSocket();

        // independent channels

        // TODO how do I know which one is for event and data?
        //  maybe after the schema is sent?
        //  this way I have to defer the creation of the handler to a later time?

        // handler for events  ---  this requires a data frame not so big
        handleSocketClient();

        // TODO handler for data  --- this requires a data frame bigger

        while( !isStopped() ){




            // TODO handle coordinator events
            //  // sidecar could do some local scheduling according to the global schedule received from the global scheduler

            // TODO push data to the sdk-runtime ????
            //  it is up to the client handler to do it, here we deal with the coordinator only

        }

        this.clientHandlerPool.shutdown();
        logger.info("VMSServer Server stopped.");

    }

    private void handleSocketClient() {

        // not blocking the vms server
        // TODO do we need a particular executor for this?
        //      yes if we want to have more control over the number of available workers for clients
        if(socketClientFuture == null){
            // I don't need to explicitly program an async accept, since the Java API gives me that by design
            socketClientFuture = serverSocket.accept();
        } else if( socketClientFuture.isDone() ){
            try {
                AsynchronousSocketChannel clientSocket = socketClientFuture.get();

                if(clientSocket == null) {
                    socketClientFuture = null;
                    return;
                }

                // store socket
                connectedClients.put(clientSocket.getRemoteAddress(), clientSocket);

                logger.info("Creating new client handler.");

                clientHandlerPool.submit(new AsyncSocketEventHandler(this, clientSocket));
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
