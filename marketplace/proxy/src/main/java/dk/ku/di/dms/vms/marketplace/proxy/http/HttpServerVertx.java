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

public final class HttpServerVertx extends AbstractVerticle {

    static Coordinator COORD;

    // https://github.com/vert-x3/vertx-examples/blob/4.x/virtual-threads-examples/src/main/java/io/vertx/example/virtualthreads/HttpClientExample.java
    static void init(Properties properties, Coordinator coordinator) {
        COORD = coordinator;
        String executor = properties.getProperty("executor");
        int http_port = Integer.parseInt( properties.getProperty("http_port") );
        int num_vertices = Integer.parseInt( properties.getProperty("num_vertices") );
        JsonObject json = new JsonObject();
        json.put("http_port", http_port);

        ThreadingModel threadingModel;
        if(executor.equals("vthread")) {
            threadingModel = ThreadingModel.VIRTUAL_THREAD;
        } else {
            // leads to higher throughput
            threadingModel = ThreadingModel.EVENT_LOOP;
        }

        boolean nativeTransport = Boolean.parseBoolean( properties.getProperty("native_transport") );
        Vertx vertx = Vertx.vertx(new VertxOptions().setPreferNativeTransport(nativeTransport));
        boolean usingNative = vertx.isNativeTransportEnabled();
        System.out.println("Vertx is running with native: " + usingNative);

        var deploymentOptions = new DeploymentOptions()
                                    .setThreadingModel(threadingModel)
                                    .setConfig(json);

        // https://vertx.io/docs/vertx-core/java/#_specifying_number_of_verticle_instances
        if(num_vertices > 1){
            // deploymentOptions.setWorkerPoolSize(num_http_workers);
            deploymentOptions.setInstances(num_vertices);
        }

        try {
            vertx.deployVerticle(HttpServerVertx.class,
                        deploymentOptions
                )
                .toCompletionStage()
                .toCompletableFuture()
                .get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace(System.out);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start(Promise<Void> startPromise) {
        HttpServerOptions options = new HttpServerOptions();
        JsonObject config = this.vertx.getOrCreateContext().config();
        options.setPort(config.getInteger("http_port"));

        options.setHost("localhost");
        options.setTcpKeepAlive(true);

        HttpServer server = this.vertx.createHttpServer(options);

        // only with linux native transport
        // does not improve considering a single driver worker
        // options.setTcpQuickAck(true);
        // need to study about
        // options.setTcpCork()

        // https://vertx.io/docs/vertx-core/java/#_asynchronous_verticle_start_and_stop
        server.requestHandler(new VertxHandler(COORD));
        if(this.vertx.getOrCreateContext().isEventLoopContext()){
            server.listen(res -> {
                if (res.succeeded()) {
                    startPromise.complete();
                } else {
                    startPromise.fail(res.cause());
                }
            });
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
                exchange.response().end(buf).onComplete(ignored -> BUFFER_POOL.add(buf));
                return;
            }

            exchange.bodyHandler(buff -> {
                String payload = buff.toString(StandardCharsets.UTF_8);
                switch (uriSplit[1]) {
                    case "cart": {
                        // assert exchange.getRequestMethod().equals("POST");
                        this.submitCustomerCheckout(payload);
                        break;
                    }
                    case "product": {
                        switch (exchange.method().name()) {
                            // price update
                            case "PATCH": {
                                this.submitUpdatePrice(payload);
                                break;
                            }
                            // product update
                            case "PUT": {
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
