package dk.ku.di.dms.vms.coordinator.http;

import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.coordinator.server.http.EdgeHttpServerBuilder;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.concurrent.*;

public class HttpServerTest {

    static ExecutorService executor;
    static HttpServer httpServer;

    static LinkedBlockingQueue<TransactionInput> queue;

    @BeforeClass
    public static void beforeAll() throws IOException {
        executor = Executors.newFixedThreadPool(1);
        BlockingQueue<TransactionInput> parsedTransactionRequests = new LinkedBlockingDeque<>();
        Map<String, TransactionDAG> transactionMap = new ConcurrentHashMap<>();
        httpServer = EdgeHttpServerBuilder.start(VmsSerdesProxyBuilder.build(), parsedTransactionRequests, transactionMap,
                new InetSocketAddress(InetAddress.getByName("localhost"), 8001));
        httpServer.setExecutor(executor);
        httpServer.start();
    }

    @AfterClass
    public static void afterAll() throws InterruptedException {
        httpServer.start();
        executor.shutdown();
    }

    @Test
    public void basicSinglePostReceive(){

        HttpClient client = HttpClient.newHttpClient();
        var request = HttpRequest.newBuilder().uri(URI.create("http://localhost:8001/data")).POST(HttpRequest.BodyPublishers.ofString("TESTE")).build();
        try {
            client.send( request, HttpResponse.BodyHandlers.discarding());
            assert true;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

}
