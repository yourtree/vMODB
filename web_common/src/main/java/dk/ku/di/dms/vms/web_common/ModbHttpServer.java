package dk.ku.di.dms.vms.web_common;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

public abstract class ModbHttpServer extends StoppableRunnable {

    protected static final List<HttpReadCompletionHandler> SSE_CLIENTS = new ArrayList<>();

    private static final ExecutorService BACKGROUND_EXECUTOR = Executors.newSingleThreadExecutor();

    protected static final List<Consumer<Long>> BATCH_COMMIT_CONSUMERS = new ArrayList<>();

    private static final Set<Future<?>> TRACKED_FUTURES = ConcurrentHashMap.newKeySet();

    protected static void submitBackgroundTask(Runnable task){
        TRACKED_FUTURES.add(BACKGROUND_EXECUTOR.submit(task));
    }

    protected static void cancelBackgroundTasks(){
        for (Future<?> future : TRACKED_FUTURES) {
            future.cancel(false);
        }
        TRACKED_FUTURES.clear();
    }

    protected static final class HttpReadCompletionHandler implements CompletionHandler<Integer, Integer> {

        private final ConnectionMetadata connectionMetadata;
        private final ByteBuffer readBuffer;
        private final ByteBuffer writeBuffer;
        private final IHttpHandler httpHandler;

        private final DefaultWriteCH defaultWriteCH = new DefaultWriteCH();
        private final CloseWriteCH closeWriteCH = new CloseWriteCH();
        private final BigBbWriteCH bigBbWriteCH = new BigBbWriteCH();
        private final SseWriteCH sseWriteCH;

        public HttpReadCompletionHandler(ConnectionMetadata connectionMetadata,
                                         ByteBuffer readBuffer, ByteBuffer writeBuffer,
                                         IHttpHandler httpHandler) {
            this.connectionMetadata = connectionMetadata;
            this.readBuffer = readBuffer;
            this.writeBuffer = writeBuffer;
            this.httpHandler = httpHandler;
            this.sseWriteCH = new SseWriteCH(this);
        }

        private record HttpRequestInternal(String httpMethod, Map<String, String> headers, String uri, String body) {}

        private static HttpRequestInternal parseRequest(String request){
            String[] requestLines = request.split("\r\n");
            String requestLine = requestLines[0];  // First line is the request line
            String[] requestLineParts = requestLine.split(" ");
            String method = requestLineParts[0];
            String url = requestLineParts[1];
            // String httpVersion = requestLineParts[2];
            // process header
            Map<String, String> headers = new HashMap<>();
            int i = 1;
            while (requestLines.length > i && !requestLines[i].isEmpty()) {
                String[] headerParts = requestLines[i].split(": ");
                headers.put(headerParts[0], headerParts[1]);
                i++;
            }
            if(method.contentEquals("GET")){
                return new HttpRequestInternal(method, headers, url, "");
            }
            StringBuilder body = new StringBuilder();
            for (i += 1; i < requestLines.length; i++) {
                body.append(requestLines[i]).append("\r\n");
            }
            String payload = body.toString().trim();
            return new HttpRequestInternal(method, headers, url, payload);
        }

        private static String createHttpHeaders(int contentLength) {
            return "HTTP/1.1 200 OK\r\n" +
                    "Content-Type: application/json\r\n" +
                    "Content-Length: " + contentLength + "\r\n" +
                    "Connection: keep-alive\r\n\r\n";
        }

        private static final byte[] OK_RESPONSE_BYTES = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".getBytes(StandardCharsets.UTF_8);

        private static final String UNKNOWN_ACCEPT = "Accept header value is not supported";

        private static final byte[] ERROR_RESPONSE_BYTES = ("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: "+UNKNOWN_ACCEPT.length()+"\r\n\r\n"+UNKNOWN_ACCEPT).getBytes(StandardCharsets.UTF_8);

        private static final byte[] NO_ACCEPT_IN_HEADER_ERR_MSG =
                ("HTTP/1.1 400 Bad Request\r\n"
                + "Content-Type: text/plain\r\n"
                + "Content-Length: 24\r\n"
                + "Connection: close\r\n"
                + "\r\n"
                + "No accept type in header").getBytes(StandardCharsets.UTF_8);

        public void process(String request){
            try {
                final HttpRequestInternal httpRequest = parseRequest(request);
                switch (httpRequest.httpMethod()){
                    case "GET" -> {
                        if(!httpRequest.headers.containsKey("Accept")){
                            this.sendErrorMsgAndCloseConnection(NO_ACCEPT_IN_HEADER_ERR_MSG);
                        } else {
                            switch (httpRequest.headers.get("Accept")) {
                                case "*/*", "application/json" -> ForkJoinPool.commonPool().submit(() -> {
                                    String dashJson = this.httpHandler.getAsJson(httpRequest.uri());
                                    byte[] dashJsonBytes = dashJson.getBytes(StandardCharsets.UTF_8);
                                    String headers = createHttpHeaders(dashJsonBytes.length);
                                    byte[] headerBytes = headers.getBytes(StandardCharsets.UTF_8);
                                    // ask memory utils for a byte buffer big enough to fit the seller dashboard
                                    int totalBytes = headerBytes.length + dashJsonBytes.length;
                                    // use remaining to be error-proof
                                    if (this.writeBuffer.remaining() < totalBytes) {
                                        ByteBuffer bigBB = MemoryManager.getTemporaryDirectBuffer(MemoryUtils.nextPowerOfTwo(totalBytes));
                                        bigBB.put(headerBytes);
                                        bigBB.put(dashJsonBytes);
                                        bigBB.flip();
                                        this.connectionMetadata.channel.write(bigBB, bigBB, this.bigBbWriteCH);
                                    } else {
                                        if (this.writeBuffer.position() != 0) {
                                            System.out.println("This buffer has not been cleaned appropriately!");
                                            this.writeBuffer.clear();
                                        }
                                        this.writeBuffer.put(headerBytes);
                                        this.writeBuffer.put(dashJsonBytes);
                                        this.writeBuffer.flip();
                                        this.connectionMetadata.channel.write(this.writeBuffer, null, this.defaultWriteCH);
                                    }
                                });
                                case "application/octet-stream" -> {
                                    byte[] byteArray = this.httpHandler.getAsBytes(httpRequest.uri());
                                    String headers = "HTTP/1.1 200 OK\r\nContent-Length: " + byteArray.length +
                                            "\r\nContent-Type: application/octet-stream\r\n\r\n";
                                    this.writeBuffer.put(headers.getBytes(StandardCharsets.UTF_8));
                                    this.writeBuffer.put(byteArray);
                                    this.writeBuffer.flip();
                                    this.connectionMetadata.channel.write(this.writeBuffer, null, this.defaultWriteCH);
                                }
                                case "text/event-stream" -> {
                                    this.processSseClient();
                                    return;
                                }
                                case null, default -> {
                                    this.sendErrorMsgAndCloseConnection(ERROR_RESPONSE_BYTES);
                                    return;
                                }
                            }
                        }
                    }
                    case "POST" -> {
                        this.httpHandler.post(httpRequest.uri(), httpRequest.body());
                        this.writeBuffer.put(OK_RESPONSE_BYTES);
                        this.writeBuffer.flip();
                        this.connectionMetadata.channel.write(this.writeBuffer, null, defaultWriteCH);
                    }
                    case "PATCH" -> {
                        if(httpRequest.uri().contains("reset")) {
                            cancelBackgroundTasks();
                        }
                        this.httpHandler.patch(httpRequest.uri(), httpRequest.body());
                        this.writeBuffer.put(OK_RESPONSE_BYTES);
                        this.writeBuffer.flip();
                        this.connectionMetadata.channel.write(this.writeBuffer, null, defaultWriteCH);
                    }
                    case "PUT" -> {
                        this.httpHandler.put(httpRequest.uri(), httpRequest.body());
                        this.writeBuffer.put(OK_RESPONSE_BYTES);
                        this.writeBuffer.flip();
                        this.connectionMetadata.channel.write(this.writeBuffer, null, defaultWriteCH);
                    }
                }
                this.readBuffer.clear();
                this.connectionMetadata.channel.read(this.readBuffer, 0, this);
            } catch (Exception e){
                // LOGGER.log(WARNING, me.identifier+": Error caught in HTTP handler.\n"+e);
                this.writeBuffer.clear();
                byte[] errorBytes;
                if(e.getMessage() == null){
                    System.out.println("Exception without message has been caught:\n"+e);
                    e.printStackTrace(System.out);
                    errorBytes = ERROR_RESPONSE_BYTES;
                } else {
                    errorBytes = ("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: "+
                            e.getMessage().length()+"\r\n\r\n" + e.getMessage()).getBytes(StandardCharsets.UTF_8);
                }
                this.writeBuffer.put(errorBytes);
                this.writeBuffer.flip();
                this.connectionMetadata.channel.write(this.writeBuffer, null, this.defaultWriteCH);
                this.readBuffer.clear();
                this.connectionMetadata.channel.read(this.readBuffer, 0, this);
            }
        }

        private void sendErrorMsgAndCloseConnection(byte[] errorResponseBytes) throws IOException {
            this.writeBuffer.put(errorResponseBytes);
            this.writeBuffer.flip();
            this.connectionMetadata.channel.write(this.writeBuffer, null, this.closeWriteCH);
            this.readBuffer.clear();
            MemoryManager.releaseTemporaryDirectBuffer(this.readBuffer);
            this.connectionMetadata.channel.close();
        }

        private void processSseClient() throws InterruptedException, ExecutionException {
            // Write HTTP response headers for SSE
            String headers = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n";
            this.writeBuffer.put(headers.getBytes(StandardCharsets.UTF_8));
            this.writeBuffer.flip();
            do {
                this.connectionMetadata.channel.write(this.writeBuffer).get();
            } while(this.writeBuffer.hasRemaining());
            // need to set up read before adding this connection to sse client
            this.writeBuffer.clear();
            this.readBuffer.clear();
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
            synchronized (SSE_CLIENTS) {
                SSE_CLIENTS.add(this);
                if (SSE_CLIENTS.size() == 1) {
                    BATCH_COMMIT_CONSUMERS.add(aLong -> SSE_CLIENTS.get(0).sendToSseClient(aLong));
                }
            }
        }

        public void sendToSseClient(long numTIDsCommitted){
            String eventData = "data: " + numTIDsCommitted + "\n\n";
            this.writeBuffer.put(eventData.getBytes(StandardCharsets.UTF_8));
            this.writeBuffer.flip();
            try {
                this.connectionMetadata.channel.write(this.writeBuffer, null, this.sseWriteCH);
            } catch (Exception e) {
                System.out.println("Error caught: "+e.getMessage());
                SSE_CLIENTS.remove(this);
                this.writeBuffer.clear();
            }
        }

        @Override
        public void completed(Integer result, Integer attachment) {
            if(result == -1){
                this.readBuffer.clear();
                this.writeBuffer.clear();
                MemoryManager.releaseTemporaryDirectBuffer(this.readBuffer);
                MemoryManager.releaseTemporaryDirectBuffer(this.writeBuffer);
                // LOGGER.log(DEBUG,me.identifier+": HTTP client has disconnected!");
                SSE_CLIENTS.remove(this);
                return;
            }
            this.readBuffer.flip();
            String request = StandardCharsets.UTF_8.decode(this.readBuffer).toString();
            this.process(request);
        }

        @Override
        public void failed(Throwable exc, Integer attachment) {
            this.readBuffer.clear();
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }

        private final class CloseWriteCH implements CompletionHandler<Integer, Void> {
            @Override
            public void completed(Integer result, Void ignored) {
                if(writeBuffer.hasRemaining()) {
                    connectionMetadata.channel.write(writeBuffer, null, this);
                    return;
                }
                writeBuffer.clear();
                MemoryManager.releaseTemporaryDirectBuffer(writeBuffer);
            }
            @Override
            public void failed(Throwable exc, Void ignored) {
                writeBuffer.clear();
                MemoryManager.releaseTemporaryDirectBuffer(writeBuffer);
            }
        }

        private final class DefaultWriteCH implements CompletionHandler<Integer, Void> {
            @Override
            public void completed(Integer result, Void ignored) {
                if(writeBuffer.hasRemaining()) {
                    connectionMetadata.channel.write(writeBuffer, null, this);
                    return;
                }
                writeBuffer.clear();
            }
            @Override
            public void failed(Throwable exc, Void ignored) {
                writeBuffer.clear();
            }
        }

        private final class SseWriteCH implements CompletionHandler<Integer, Void> {
            private final HttpReadCompletionHandler r;
            public SseWriteCH(HttpReadCompletionHandler r){
                this.r = r;
            }
            @Override
            public void completed(Integer result, Void ignored) {
                if(writeBuffer.hasRemaining()) {
                    connectionMetadata.channel.write(writeBuffer, null, this);
                    return;
                }
                writeBuffer.clear();
            }
            @Override
            public void failed(Throwable exc, Void ignored) {
                writeBuffer.clear();
                SSE_CLIENTS.remove(this.r);
            }
        }

        private final class BigBbWriteCH implements CompletionHandler<Integer, ByteBuffer> {
            @Override
            public void completed(Integer result, ByteBuffer byteBuffer) {
                if(byteBuffer.hasRemaining()) {
                    connectionMetadata.channel.write(byteBuffer, byteBuffer, this);
                    return;
                }
                byteBuffer.clear();
                MemoryManager.releaseTemporaryDirectBuffer(byteBuffer);
            }
            @Override
            public void failed(Throwable exc, ByteBuffer byteBuffer) {
                byteBuffer.clear();
                MemoryManager.releaseTemporaryDirectBuffer(byteBuffer);
            }
        }
    }

    protected static boolean isHttpClient(String request) {
        String subStr = request.substring(0, Math.max(request.indexOf(' '), 0));
        switch (subStr){
            case "GET", "PATCH", "POST", "PUT" -> {
                return true;
            }
        }
        return false;
    }

    /**
     * Useful for managed experiments, that is, when workload is generated in place
     */
    public void registerBatchCommitConsumer(Consumer<Long> consumer){
        BATCH_COMMIT_CONSUMERS.add(consumer);
    }

}

