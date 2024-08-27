package dk.ku.di.dms.vms.marketplace.cart.infra;

import dk.ku.di.dms.vms.marketplace.cart.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;
import io.vertx.core.*;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

public final class CartHttpServerVertx extends AbstractVerticle {

    static VmsApplication CART_VMS;
    static AbstractProxyRepository<CartItem.CartItemId, CartItem> CART_REPO;
    static IVmsSerdesProxy SERDES = VmsSerdesProxyBuilder.build();

    @SuppressWarnings("unchecked")
    public static void init(VmsApplication cartVms, int numVertices, boolean nativeTransport){
        CART_VMS = cartVms;
        CART_REPO = (AbstractProxyRepository<CartItem.CartItemId, CartItem>) CART_VMS.getRepositoryProxy("cart_items");
        Vertx vertx = Vertx.vertx(new VertxOptions().setPreferNativeTransport(nativeTransport));
        boolean usingNative = vertx.isNativeTransportEnabled();
        System.out.println("Vertx is running with native: " + usingNative);
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
    }

    @Override
    public void start(Promise<Void> startPromise) {
        HttpServerOptions options = new HttpServerOptions();
        options.setPort(Constants.CART_HTTP_PORT);
        options.setHost("0.0.0.0");
        options.setTcpKeepAlive(true);
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
            exchange.bodyHandler(buff -> {
                if (!uriSplit[1].equals("cart")) {
                    handleError(exchange, "Invalid URI");
                    return;
                }
                switch (exchange.method().name()) {
                    case "GET" -> {
                        String[] split = exchange.uri().split("/");
                        int customerId = Integer.parseInt(split[split.length - 3]);
                        int sellerId = Integer.parseInt(split[split.length - 2]);
                        int productId = Integer.parseInt(split[split.length - 1]);
                        try(var _ = CART_VMS.getTransactionManager().beginTransaction(0, 0, 0,true)) {
                            CartItem cartItem = CART_REPO.lookupByKey(new CartItem.CartItemId(customerId, sellerId, productId));
                            if (cartItem != null) {
                                exchange.response().setChunked(true);
                                exchange.response().setStatusCode(200);
                                exchange.response().write(cartItem.toString());
                                exchange.response().end();
                            }
                        } catch (Exception e) {
                            handleError(exchange, e.getMessage());
                        }
                    }
                    case "PATCH" -> {
                        String[] split = exchange.uri().split("/");
                        int customerId = Integer.parseInt(split[split.length - 2]);
                        String payload = buff.toString(StandardCharsets.UTF_8);
                        dk.ku.di.dms.vms.marketplace.common.entities.CartItem cartItemAPI =
                                SERDES.deserialize(payload,
                                        dk.ku.di.dms.vms.marketplace.common.entities.CartItem.class);
                        long tid = CART_VMS.lastTidFinished();
                        try(var _ = CART_VMS.getTransactionManager().beginTransaction(tid, 0, 0, false)){
                            CART_REPO.insert(CartUtils.convertCartItemAPI(customerId, cartItemAPI));
                            CART_VMS.getTransactionManager().commit();
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
