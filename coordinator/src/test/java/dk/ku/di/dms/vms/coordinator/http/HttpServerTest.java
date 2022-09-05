package dk.ku.di.dms.vms.coordinator.http;

import dk.ku.di.dms.vms.coordinator.server.http.HttpEventLoop;
import dk.ku.di.dms.vms.coordinator.server.http.Options;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.web_common.serdes.VmsSerdesProxyBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class HttpServerTest {

    static ExecutorService executor;
    static int port;
    static HttpEventLoop eventLoop;
    static Thread eventLoopThread;

    static LinkedBlockingQueue<TransactionInput> queue;

    // Should we accept keep alive? maybe yes, the same client may send other transactions....
    static final String HTTP10_KEEP_ALIVE_REQUEST = """
            GET /file HTTP/1.0\r
            Connection: Keep-Alive\r
            \r
            """;

    static final String HTTP10_KEEP_ALIVE_RESPONSE = """
            HTTP/1.0 200 OK\r
            Connection: Keep-Alive\r
            Content-Length: 12\r
            Content-Type: text/plain\r
            \r
            hello world
            """;

    @BeforeClass
    static void beforeAll() throws IOException {
        executor = Executors.newFixedThreadPool(1);
        Options options = new Options()
                .withPort(0)
                .withRequestTimeout(Duration.ofMillis(2_500))
                .withReadBufferSize(1_024)
                .withMaxRequestSize(2_048);

        queue = new LinkedBlockingQueue<>();

        eventLoop = new HttpEventLoop(options, VmsSerdesProxyBuilder.build(), queue);

        port = eventLoop.getPort();
        eventLoopThread = new Thread(eventLoop);
        eventLoopThread.start();
    }

    @AfterClass
    static void afterAll() throws InterruptedException {
        eventLoop.stop();
        eventLoopThread.join();
        executor.shutdown();
    }

    @Test
    public void basicSinglePostReceive(){



    }



}
