package dk.ku.di.dms.vms.coordinator.server.http;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

class Connection {

    static final String HTTP_1_0 = "HTTP/1.0";
    static final String HTTP_1_1 = "HTTP/1.1";

    static final String HEADER_CONNECTION = "Connection";
    static final String HEADER_CONTENT_LENGTH = "Content-Length";

    static final String KEEP_ALIVE = "Keep-Alive";

    private final SocketChannel socketChannel;
    private final SelectionKey selectionKey;

    private final ByteTokenizer byteTokenizer;

    private RequestParser requestParser;

    private ByteBuffer readBuffer;
    private ByteBuffer writeBuffer;

    private boolean httpOneDotZero;
    private boolean keepAlive;

    Connection(SocketChannel socketChannel, SelectionKey selectionKey, ByteBuffer readBuffer) {
        this.socketChannel = socketChannel;
        this.selectionKey = selectionKey;
        this.byteTokenizer = new ByteTokenizer();
        this.requestParser = new RequestParser(byteTokenizer);
        this.readBuffer = readBuffer;
    }

    public boolean onWritable() {
        try {
            doOnWritable();
            return true;
        } catch (IOException | RuntimeException e) {
            failSafeClose();
            return false;
        }
    }

    private void doOnWritable() throws IOException {
        int numBytes = socketChannel.write(writeBuffer);
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

        Request request = requestParser.request();
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

    public boolean onReadable() {
        try {
            doOnReadable();
            return true;
        } catch (IOException | RuntimeException e) {
            failSafeClose();
            return false;
        }
    }

    private void doOnReadable() throws IOException {
        readBuffer.clear();
        int numBytes = socketChannel.read(readBuffer);
        if (numBytes < 0) {
            failSafeClose();
            return;
        }

        // TODO parse message and deliver to leader or follower


        readBuffer.flip();
    }

    private void failSafeClose() {
        try {
            selectionKey.cancel();
            socketChannel.close();
        } catch (IOException e) {
            // suppress error
        }
    }

}

