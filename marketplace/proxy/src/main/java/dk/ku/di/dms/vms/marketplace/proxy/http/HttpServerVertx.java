package dk.ku.di.dms.vms.marketplace.proxy.http;

import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.Coordinator;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;

import static io.vertx.core.Future.await;
import static java.lang.Thread.sleep;

public final class HttpServerVertx extends AbstractVerticle {

    static Coordinator COORD;

    // https://github.com/vert-x3/vertx-examples/blob/4.x/virtual-threads-examples/src/main/java/io/vertx/example/virtualthreads/HttpClientExample.java
    public static void init(Properties properties, Coordinator coordinator) throws ExecutionException, InterruptedException {
        COORD = coordinator;
        String executor = properties.getProperty("executor");
        int http_port = Integer.parseInt( properties.getProperty("http_port") );
        JsonObject json = new JsonObject();
        json.put("http_port", http_port);

        ThreadingModel threadingModel;
        if(executor.equals("vthread")) {
            threadingModel = ThreadingModel.VIRTUAL_THREAD;
        } else {
            // leads to higher throughput
            threadingModel = ThreadingModel.EVENT_LOOP;
        }

        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(HttpServerVertx.class,
                        new DeploymentOptions().setThreadingModel(threadingModel)
                                .setConfig(json)
                )
                .toCompletionStage()
                .toCompletableFuture()
                .get();
    }

    @SuppressWarnings("BusyWait")
    @Override
    public void start() {
        HttpServerOptions options = new HttpServerOptions();
        JsonObject config = vertx.getOrCreateContext().config();
        options.setPort(config.getInteger("http_port"));

        options.setHost("localhost");
        options.setTcpKeepAlive(true);

        HttpServer server = vertx.createHttpServer(options);

        // only linux native transport
        // options.setTcpQuickAck(true);

        server.requestHandler(new VertxHandler(COORD));
        if(vertx.getOrCreateContext().isEventLoopContext()){
            // wait
            do {
                try { sleep(10); } catch (InterruptedException ignored) { }
            } while(!server.listen().isComplete());
        } else {
            await(server.listen());
        }
    }

    public static class VertxHandler extends AbstractHttpHandler implements Handler<HttpServerRequest> {

        final ConcurrentLinkedDeque<Buffer> BUFFER_POOL = new ConcurrentLinkedDeque<>();

        public VertxHandler(Coordinator coordinator) {
            super(coordinator);
        }

        @Override
        public void handle(HttpServerRequest exchange) {
            String[] uriSplit = exchange.uri().split("/");
            if (uriSplit[1].equals("status")) {
                // assumed to be a get request
                // assert exchange.getRequestMethod().equals("GET");
                byte[] b = this.getLastTidBytes();

                Buffer buf = retrieveBuffer();
                buf.setBytes(0, b);

                // b.length always equals to 8
                //Buffer finalBuf = buf;
                exchange.response().setStatusCode(200);
                exchange.response().end(buf).onComplete(_ -> BUFFER_POOL.add(buf));
                return;
            }

            exchange.bodyHandler(buff -> {
                switch (uriSplit[1]) {
                    case "cart": {
                        // assert exchange.getRequestMethod().equals("POST");
                        String payload = buff.toString(StandardCharsets.UTF_8);
                        this.submitCustomerCheckout(payload);
                        break;
                    }
                    case "product": {
                        switch (exchange.method().name()) {
                            // price update
                            case "PATCH": {
                                String payload = buff.toString(StandardCharsets.UTF_8);
                                this.submitUpdatePrice(payload);
                                break;
                            }
                            // product update
                            case "PUT": {
                                String payload = buff.toString(StandardCharsets.UTF_8);
                                this.submitUpdateProduct(payload);
                                break;
                            }
                            default: {
                                exchange.response().setStatusCode(500).end();
                                return;
                            }
                        }
                        break;
                    }
                    case "shipment": {
                        // assert exchange.getRequestMethod().equals("PATCH");
                        String payload = buff.toString(StandardCharsets.UTF_8);
                        this.submitUpdateDelivery(payload);
                        break;
                    }
                    default: {
                        exchange.response().setStatusCode(500).end();
                        return;
                    }
                }
                exchange.response().setStatusCode(200).end();
            });
        }

        private Buffer retrieveBuffer() {
            Buffer buf = BUFFER_POOL.poll();
            if(buf == null) {
                buf = Buffer.buffer(8);
            }
            return buf;
        }

    }

}
