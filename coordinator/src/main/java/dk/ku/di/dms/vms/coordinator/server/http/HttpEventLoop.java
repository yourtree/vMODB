package dk.ku.di.dms.vms.coordinator.server.http;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;

import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.TCP_NODELAY;

/**
 * This event loop targets at allowing external clients to communicate via HTTP protocol.
 * Mainly for receiving transaction requests.
 * Based on micro-http project: https://github.com/ebarlas/microhttp
 * Removed the scheduler, task queue, and logging.
 *
 * https://josephmate.github.io/2022-04-14-max-connections/
 *
 *  Could just use the default java http server (with code for TLS)
 * {@link com.sun.net.httpserver.HttpServer}
 * Have to make experiments to see which one is faster
 */
public class HttpEventLoop implements Runnable {

    private final Selector selector;
    private final ServerSocketChannel serverSocketChannel;

    private final Options options;

    private final ByteBuffer readBuffer;

    private volatile boolean stop;

    private final Map<SelectionKey, Connection> connectionMap;

    // this is the producer, single-thread. the consumer is also a single-thread (the leader)
    private final BlockingQueue<byte[]> queue;

    public HttpEventLoop(Options options, BlockingQueue<byte[]> queue) throws IOException {

        // set maximum number of connections this socket can handle per second
        this.connectionMap = new HashMap<>();

        this.queue = queue;
        this.stop = false;
        this.options = options;

        readBuffer = ByteBuffer.allocateDirect(options.readBufferSize());
        selector = Selector.open();

        InetSocketAddress address = options.host() == null
                ? new InetSocketAddress(options.port()) // wildcard address
                : new InetSocketAddress(options.host(), options.port());

        serverSocketChannel = ServerSocketChannel.open();
        if (options.reuseAddr()) {
            serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, options.reuseAddr());
        }
        if (options.reusePort()) {
            serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEPORT, options.reusePort());
        }
        serverSocketChannel.configureBlocking(options.blocking());

        serverSocketChannel.setOption( SO_KEEPALIVE, options.keepAlive() );
        serverSocketChannel.setOption( TCP_NODELAY, options.noDelay() );

        serverSocketChannel.bind(address, options.acceptLength());
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

    }

    public int getPort() throws IOException {
        return serverSocketChannel.getLocalAddress() instanceof InetSocketAddress a ? a.getPort() : -1;
    }

    public void stop() {
        stop = true;
    }

    @Override
    public void run() {

        try {
            doStart();
        } catch (IOException ignored) { }

    }

    /**
     * https://www.oreilly.com/library/view/java-nio/0596002882/ch04.html
     * TODO should have a mechanism to restart this task upon failure... an exception handler would do the work
     * @throws IOException from select operation
     */
    public void doStart() throws IOException {

        while (!stop) {
            selector.select(options.resolution().toMillis());
            Set<SelectionKey> selectedKeys = selector.selectedKeys();
            Iterator<SelectionKey> it = selectedKeys.iterator();
            while (it.hasNext()) {
                SelectionKey selectionKey = it.next();
                if (selectionKey.isAcceptable()) {
                    onAcceptable();
                } else if (selectionKey.isReadable()) {
                    connectionMap.get( selectionKey ).onReadable();
                } else if (selectionKey.isWritable()) {
                    connectionMap.get( selectionKey ).onWritable();
                }
                it.remove();
            }

        }

    }

    private void onAcceptable() throws IOException {
        SocketChannel socketChannel = serverSocketChannel.accept();
        socketChannel.configureBlocking(options.blocking());

        socketChannel.setOption( SO_KEEPALIVE, options.keepAlive() );
        socketChannel.setOption( TCP_NODELAY, options.noDelay() );

        SelectionKey selectionKey = socketChannel.register(selector, SelectionKey.OP_READ);

        // create new connection
        Connection connection = new Connection(socketChannel, selectionKey);
        connectionMap.put( selectionKey, connection );
    }

    // dependencies on event loop class: readBuffer, Options, and the connectionMap
    private class Connection {

        static final String HTTP_1_0 = "HTTP/1.0";
        static final String HTTP_1_1 = "HTTP/1.1";

        static final String HEADER_CONNECTION = "Connection";
        static final String HEADER_CONTENT_LENGTH = "Content-Length";

        static final String KEEP_ALIVE = "Keep-Alive";

        private final SocketChannel socketChannel;
        private final SelectionKey selectionKey;

        private final ByteTokenizer byteTokenizer;

        private RequestParser requestParser;

        private ByteBuffer writeBuffer;

        private boolean httpOneDotZero;
        private boolean keepAlive;

        Connection(SocketChannel socketChannel, SelectionKey selectionKey) {
            this.socketChannel = socketChannel;
            this.selectionKey = selectionKey;
            this.byteTokenizer = new ByteTokenizer();
            this.requestParser = new RequestParser(byteTokenizer);
        }

        public void onWritable() {
            try {
                doOnWritable();
            } catch (IOException | RuntimeException e) {
                failSafeClose();
            }
        }

        private void doOnWritable() throws IOException {
            // int numBytes = socketChannel.write(writeBuffer);
            if (!writeBuffer.hasRemaining()) { // response fully written
                writeBuffer = null; // done with current write buffer, remove reference
                if (httpOneDotZero && !keepAlive) { // non-persistent connection, close now
                    failSafeClose();
                } else { // persistent connection
                    if (requestParser.parse()) { // subsequent request in buffer
                        onParseRequest();
                    } else { // switch back to read mode
                        selectionKey.interestOps(SelectionKey.OP_READ);
                    }
                }
            } else { // response not fully written, switch to or remain in write mode
                if ((selectionKey.interestOps() & SelectionKey.OP_WRITE) == 0) {
                    selectionKey.interestOps(SelectionKey.OP_WRITE);
                }
            }
        }

        private void onParseRequest() throws IOException {
            if (selectionKey.interestOps() != 0) {
                selectionKey.interestOps(0);
            }

            // parse message and deliver to leader or follower
            Request request = requestParser.request();

            // put into a queue for leader consumption
            queue.add(request.body());

            httpOneDotZero = request.version().equalsIgnoreCase(HTTP_1_0);
            keepAlive = request.hasHeader(HEADER_CONNECTION, KEEP_ALIVE);
            byteTokenizer.compact();
            requestParser = new RequestParser(byteTokenizer);

            Response response = new Response(
                    200,
                    "OK",
                    List.of(new Header("Content-Type", "text/plain")),
                    "".getBytes(StandardCharsets.UTF_8));

            prepareToWriteResponse(response);
        }

        private void prepareToWriteResponse(Response response) throws IOException {
            String version = httpOneDotZero ? HTTP_1_0 : HTTP_1_1;
            List<Header> headers = new ArrayList<>();
            if (httpOneDotZero && keepAlive) {
                headers.add(new Header(HEADER_CONNECTION, KEEP_ALIVE));
            }
            if (!response.hasHeader(HEADER_CONTENT_LENGTH)) {
                headers.add(new Header(HEADER_CONTENT_LENGTH, Integer.toString(response.body().length)));
            }
            writeBuffer = ByteBuffer.wrap(response.serialize(version, headers));

            doOnWritable();
        }

        public void onReadable() {
            try {
                doOnReadable();
            } catch (IOException | RuntimeException e) {
                failSafeClose();
            }
        }

        private void doOnReadable() throws IOException {
            readBuffer.clear();
            int numBytes = socketChannel.read(readBuffer);
            if (numBytes < 0) {
                failSafeClose();
                return;
            }

            readBuffer.flip();
            byteTokenizer.add(readBuffer);

            if (requestParser.parse()) {
                onParseRequest();
            } else {
                if (byteTokenizer.size() > options.maxRequestSize()) {
                    failSafeClose();
                }
            }

        }

        private void failSafeClose() {
            try {
                selectionKey.cancel();
                socketChannel.close();
            } catch (IOException e) {
                // suppress error
            } finally {
                // independently of error, remove it
                connectionMap.remove(selectionKey);
            }
        }

    }

}
