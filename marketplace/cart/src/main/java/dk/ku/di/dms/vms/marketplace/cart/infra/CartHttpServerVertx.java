package dk.ku.di.dms.vms.marketplace.cart.infra;

import dk.ku.di.dms.vms.marketplace.cart.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.cart.repositories.ICartItemRepository;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import io.vertx.core.*;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;

public final class CartHttpServerVertx extends AbstractVerticle {

    static VmsApplication CART_VMS;
    static ICartItemRepository CART_REPO;
    static IVmsSerdesProxy SERDES = VmsSerdesProxyBuilder.build();

    public static void init(VmsApplication cartVms, int numVertices, int httpThreadPoolSize, boolean nativeTransport){
        CART_VMS = cartVms;
        CART_REPO = (ICartItemRepository) CART_VMS.getRepositoryProxy("cart_items");
        VertxOptions vertxOptions = new VertxOptions().setPreferNativeTransport(nativeTransport);
        if(httpThreadPoolSize > 0){
            vertxOptions.setEventLoopPoolSize(httpThreadPoolSize);
        }
        Vertx vertx = Vertx.vertx(vertxOptions);
        boolean usingNative = vertx.isNativeTransportEnabled();
        DeploymentOptions deploymentOptions = new DeploymentOptions()
                .setThreadingModel(ThreadingModel.EVENT_LOOP)
                .setInstances(numVertices);
        try {
            vertx.deployVerticle(CartHttpServerVertx.class,
                            deploymentOptions
                    )
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace(System.out);
            throw new RuntimeException(e);
        }
        System.out.println("Vertx is running with the following options:\n" +
                "vertices: "+ numVertices +
                "\nevent loop size: "+httpThreadPoolSize +
                "\nnative: " + usingNative);
    }

    @Override
    public void start(Promise<Void> startPromise) {
        HttpServerOptions options = new HttpServerOptions();
        options.setPort(Constants.CART_HTTP_PORT);
        options.setHost("0.0.0.0");
        options.setTcpKeepAlive(true);
        options.setTcpNoDelay(true);
        HttpServer server = this.vertx.createHttpServer(options);
        server.requestHandler(new VertxHandler());
        server.listen(res -> {
            if (res.succeeded()) {
                startPromise.complete();
            } else {
                startPromise.fail(res.cause());
            }
        });
    }

    public static class VertxHandler implements Handler<HttpServerRequest> {
        @Override
        public void handle(HttpServerRequest exchange) {
            String[] uriSplit = exchange.uri().split("/");
            if (uriSplit.length < 2 || !uriSplit[1].equals("cart")) {
                handleError(exchange, "Invalid URI");
                return;
            }
            exchange.bodyHandler(buff -> {
                switch (exchange.method().name()) {
                    case "GET" -> {
                        int customerId = Integer.parseInt(uriSplit[uriSplit.length - 1]);
                        long tid = CART_VMS.lastTidFinished();
                        try(var txCtx = CART_VMS.getTransactionManager().beginTransaction(tid, 0, 0,true)) {
                            List<CartItem> cartItems = CART_REPO.getCartItemsByCustomerId(customerId);
                            exchange.response().setChunked(true);
                            exchange.response().setStatusCode(200);
                            if (cartItems != null) {
                                String payload = SERDES.serializeList(cartItems);
                                exchange.response().write(payload);
                            }
                            exchange.response().end();
                        } catch (Exception e) {
                            handleError(exchange, e.getMessage());
                        }
                    }
                    case "PATCH" -> {
                        String op = uriSplit[uriSplit.length - 1];
                        if(op.contentEquals("reset")){
                            // path: /cart/reset
                            try {
                                CART_VMS.getTransactionManager().reset();
                                exchange.response().setStatusCode(200).end();
                            } catch (Exception e){
                                handleError(exchange, e.getMessage());
                            }
                            return;
                        }
                        // path: /cart/<customer_id>/add
                        int customerId = Integer.parseInt(uriSplit[uriSplit.length - 2]);
                        String payload = buff.toString(StandardCharsets.UTF_8);
                        dk.ku.di.dms.vms.marketplace.common.entities.CartItem cartItemAPI =
                                SERDES.deserialize(payload, dk.ku.di.dms.vms.marketplace.common.entities.CartItem.class);
                        long tid = CART_VMS.lastTidFinished();
                        try(var txCtx = CART_VMS.getTransactionManager().beginTransaction(tid, 0, 0, false)){
                            CART_REPO.insert(CartUtils.convertCartItemAPI(customerId, cartItemAPI));
                            // prevent conflicting with other threads on installing writes
                            // CART_VMS.getTransactionManager().commit();
                            exchange.response().setStatusCode(200).end();
                        } catch (Exception e){
                            handleError(exchange, e.getMessage());
                        }
                    }
                    default -> handleError(exchange, "Invalid URI");
                }
            });
        }
    }

    private static void handleError(HttpServerRequest exchange, String msg) {
        exchange.response().setChunked(true);
        exchange.response().setStatusCode(500);
        exchange.response().write(msg);
        exchange.response().end();
    }

}
