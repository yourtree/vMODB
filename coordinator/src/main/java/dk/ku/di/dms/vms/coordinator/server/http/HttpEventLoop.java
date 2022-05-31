package dk.ku.di.dms.vms.coordinator.server.http;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.TCP_NODELAY;

/**
 * This event loop targets at allowing external clients to communicate via HTTP protocol.
 * Mainly for receiving transaction requests.
 * Based on micro-http project: https://github.com/ebarlas/microhttp
 * Removed the scheduler, task queue, and logging
 */
public class HttpEventLoop implements Runnable {

    private final Selector selector;
    private final ServerSocketChannel serverSocketChannel;

    private final Options options;

    private final ByteBuffer readBuffer;

    private volatile boolean stop;

    private Map<SelectionKey, Connection> connectionMap;

    public HttpEventLoop(Options options) throws IOException {

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


    @Override
    public void run() {

        try {
            doStart();
        } catch (IOException e) {

        }


    }

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
                    // if false, error caught. instead of throwing the exception up, returning boolean gives less overhead
                    if(!connectionMap.get( selectionKey ).onReadable()){
                        connectionMap.remove( selectionKey );
                    }
                } else if (selectionKey.isWritable()) {
                    if(!connectionMap.get( selectionKey ).onWritable()){
                        connectionMap.remove( selectionKey );
                    }
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
        Connection connection = new Connection(socketChannel, selectionKey, readBuffer);
        connectionMap.put( selectionKey, connection );
    }

}
