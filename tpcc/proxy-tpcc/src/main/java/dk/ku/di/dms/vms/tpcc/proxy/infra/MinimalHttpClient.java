package dk.ku.di.dms.vms.tpcc.proxy.infra;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public final class MinimalHttpClient implements Closeable {

    private final SocketChannel socketChannel;
    private final ByteBuffer writeBuffer;
    private final ByteBuffer readBuffer;

    private StringBuilder response;

    public MinimalHttpClient(String hostname, int port) throws IOException {
        this.socketChannel = SocketChannel.open();
        this.socketChannel.configureBlocking(true);
        this.socketChannel.connect(new InetSocketAddress(hostname, port));
        this.writeBuffer = MemoryManager.getTemporaryDirectBuffer(MemoryUtils.DEFAULT_PAGE_SIZE);
        this.readBuffer = MemoryManager.getTemporaryDirectBuffer(MemoryUtils.DEFAULT_PAGE_SIZE);
    }

    private static String buildHttpRequest(String method, String payload, int length, String param){
        return method+" /"+param+" HTTP/1.1\r\n"
                + "Host: proxy-http\r\n"
                + "Content-Type: application/json\r\n"
                + "Content-Length: " + length + "\r\n"
                + "\r\n"
                + payload; // + "\r\n"; // not necessary if following the req/rep http protocol
    }

    private static String buildHttpGetRequest(String param){
        return "GET /"+param+" HTTP/1.1\r\n"
                + "Host: proxy-http\r\n"
                + "Accept: application/json\r\n"
                + "\r\n";
    }

    public String sendGetRequest(String param) throws IOException {
        String httpRequest = buildHttpGetRequest(param);
        this.writeBuffer.put(httpRequest.getBytes(StandardCharsets.UTF_8));
        this.writeBuffer.flip();
        this.socketChannel.write(this.writeBuffer);
        this.writeBuffer.clear();

        if(this.response == null) {
            this.response = new StringBuilder();
        } else {
            response.setLength(0);
        }

        // read to block waiting for response and avoid concatenating requests
        this.socketChannel.read(this.readBuffer);
        this.readBuffer.flip();
        while (readBuffer.hasRemaining()) {
            response.append((char) readBuffer.get());
        }
        this.readBuffer.clear();
        return response.toString();
    }

    public void sendRequest(String method, String jsonBody, String param) throws IOException {
        String httpRequest = buildHttpRequest(method, jsonBody, jsonBody.getBytes(StandardCharsets.UTF_8).length, param);
        this.writeBuffer.put(httpRequest.getBytes(StandardCharsets.UTF_8));
        this.writeBuffer.flip();
        this.socketChannel.write(this.writeBuffer);
        this.writeBuffer.clear();

        // read to block waiting for response and avoid concatenating requests
        this.socketChannel.read(this.readBuffer);
//        this.readBuffer.flip();
////            while (readBuffer.hasRemaining()) {
////                response.append((char) readBuffer.get());
////            }
        this.readBuffer.clear();
//        }
//        System.out.println(response);
//        response.setLength(0);
    }

    @Override
    public void close() throws IOException {
        this.socketChannel.close();
        this.readBuffer.clear();
        this.writeBuffer.clear();
        MemoryManager.releaseTemporaryDirectBuffer(this.readBuffer);
        MemoryManager.releaseTemporaryDirectBuffer(this.writeBuffer);
    }

}
