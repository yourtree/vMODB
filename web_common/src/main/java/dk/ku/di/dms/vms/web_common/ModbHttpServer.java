package dk.ku.di.dms.vms.web_common;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public abstract class ModbHttpServer extends StoppableRunnable {

    protected static final List<HttpReadCompletionHandler> sseClients = new CopyOnWriteArrayList<>();

    protected static final class HttpReadCompletionHandler implements CompletionHandler<Integer, Integer> {

        private final ConnectionMetadata connectionMetadata;
        private final ByteBuffer readBuffer;
        private final ByteBuffer writeBuffer;
        private final IHttpHandler httpHandler;

        public HttpReadCompletionHandler(ConnectionMetadata connectionMetadata,
                                         ByteBuffer readBuffer, ByteBuffer writeBuffer,
                                         IHttpHandler httpHandler) {
            this.connectionMetadata = connectionMetadata;
            this.readBuffer = readBuffer;
            this.writeBuffer = writeBuffer;
            this.httpHandler = httpHandler;
        }

        private record HttpRequestInternal(String httpMethod, Map<String, String> headers, String uri, String body) {}

        private static HttpRequestInternal parseRequest(String request){
            String[] requestLines = request.split("\r\n");
            String requestLine = requestLines[0];  // First line is the request line
            String[] requestLineParts = requestLine.split(" ");
            String method = requestLineParts[0];
            String url = requestLineParts[1];
            String httpVersion = requestLineParts[2];
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

        public void process(String request){
            try {
                HttpRequestInternal httpRequest = parseRequest(request);
                Future<Integer> ft = null;
                switch (httpRequest.httpMethod()){
                    case "GET" -> {
                        if(!httpRequest.headers.containsKey("Accept")){
                            var noContentType = "HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: 11\r\n\r\nNo accept type in header".getBytes(StandardCharsets.UTF_8);
                            this.writeBuffer.put(noContentType);
                        } else {
                            switch (httpRequest.headers.get("Accept")) {
                                case "*/*", "application/json" -> {
                                    String respVms = httpHandler.getAsJson(httpRequest.uri());
                                    byte[] respBytes = respVms.getBytes(StandardCharsets.UTF_8);
                                    String response = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                                            + respBytes.length + "\r\n\r\n" + respVms;
                                    this.writeBuffer.put(response.getBytes(StandardCharsets.UTF_8));
                                }
                                case "application/octet-stream" -> {
                                    byte[] byteArray = httpHandler.getAsBytes(httpRequest.uri());
                                    String headers = "HTTP/1.1 200 OK\r\nContent-Length: " + byteArray.length +
                                            "\r\nContent-Type: application/octet-stream\r\n\r\n";
                                    this.writeBuffer.put(headers.getBytes(StandardCharsets.UTF_8));
                                    this.writeBuffer.put(byteArray);
                                }
                                case "text/event-stream" -> {
                                    this.processSseClient();
                                    return;
                                }
                                case null, default -> this.writeBuffer.put(ERROR_RESPONSE_BYTES);
                            }
                        }
                        this.writeBuffer.flip();
                        ft = this.connectionMetadata.channel.write(this.writeBuffer);
                    }
                    case "POST" -> {
                        httpHandler.post(httpRequest.uri(), httpRequest.body());
                        this.writeBuffer.put(OK_RESPONSE_BYTES);
                        this.writeBuffer.flip();
                        ft = this.connectionMetadata.channel.write(this.writeBuffer);
                    }
                    case "PATCH" -> {
                        httpHandler.patch(httpRequest.uri(), httpRequest.body());
                        this.writeBuffer.put(OK_RESPONSE_BYTES);
                        this.writeBuffer.flip();
                        ft = this.connectionMetadata.channel.write(this.writeBuffer);
                    }
                    case "PUT" -> {
                        httpHandler.put(httpRequest.uri(), httpRequest.body());
                        this.writeBuffer.put(OK_RESPONSE_BYTES);
                        this.writeBuffer.flip();
                        ft = this.connectionMetadata.channel.write(this.writeBuffer);
                    }
                }
                if(ft != null){
                    int result = ft.get();
                    // send remaining to avoid http client to hang
                    while (result < this.writeBuffer.position()){
                        result += this.connectionMetadata.channel.write(this.writeBuffer).get();
                    }
                }
                this.writeBuffer.clear();
            } catch (Exception e){
                // LOGGER.log(WARNING, me.identifier+": Error caught in HTTP handler.\n"+e);
                this.writeBuffer.clear();
                this.writeBuffer.put(ERROR_RESPONSE_BYTES);
                this.writeBuffer.flip();
                this.connectionMetadata.channel.write(writeBuffer);
            }
            this.readBuffer.clear();
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }

        private void processSseClient() throws InterruptedException, ExecutionException {
            // Write HTTP response headers for SSE
            String headers = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n";
            this.writeBuffer.put(headers.getBytes(StandardCharsets.UTF_8));
            this.writeBuffer.flip();
            this.connectionMetadata.channel.write(this.writeBuffer).get();
            this.writeBuffer.clear();
            this.readBuffer.clear();
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
            sseClients.add(this);
        }

        public void sendToSseClient(long numTIDsCommitted){
            String eventData = "data: " + numTIDsCommitted + "\n\n";
            this.writeBuffer.put(eventData.getBytes(StandardCharsets.UTF_8));
            this.writeBuffer.flip();
            try {
                do {
                    this.connectionMetadata.channel.write(this.writeBuffer).get();
                } while (this.writeBuffer.hasRemaining());
            } catch (InterruptedException | ExecutionException e) {
                System.out.println("Error caught: "+e.getMessage());
                sseClients.remove(this);
                // e.printStackTrace(System.out);
            }
            this.writeBuffer.clear();
        }

        private static final byte[] OK_RESPONSE_BYTES = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".getBytes(StandardCharsets.UTF_8);

        private static final byte[] ERROR_RESPONSE_BYTES = "HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: 11\r\n\r\nBad Request".getBytes(StandardCharsets.UTF_8);

        @Override
        public void completed(Integer result, Integer attachment) {
            if(result == -1){
                this.readBuffer.clear();
                this.writeBuffer.clear();
                MemoryManager.releaseTemporaryDirectBuffer(this.readBuffer);
                MemoryManager.releaseTemporaryDirectBuffer(this.writeBuffer);
                // LOGGER.log(DEBUG,me.identifier+": HTTP client has disconnected!");
                sseClients.remove(this);
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
    }

    // POST, PATCH, GET
    protected static boolean isHttpClient(String request) {
        var substr = request.substring(0, request.indexOf(' '));
        switch (substr){
            case "GET", "PATCH", "POST", "PUT" -> {
                return true;
            }
        }
        return false;
    }

}
