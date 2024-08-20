package dk.ku.di.dms.vms.marketplace.cart.infra;

import dk.ku.di.dms.vms.marketplace.cart.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.transaction.TransactionContext;
import dk.ku.di.dms.vms.modb.transaction.TransactionManager;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;
import io.vertx.core.*;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

public class CartHttpServerVertx extends AbstractVerticle {

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
        DeploymentOptions deploymentOptions = new DeploymentOptions().setThreadingModel(ThreadingModel.EVENT_LOOP);

        if(numVertices > 1){
            deploymentOptions.setInstances(numVertices);
        }

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
        options.setHost("localhost");
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
                if (uriSplit[1].equals("cart")) {
                    switch (exchange.method().name()) {
                        case "GET" -> {
                            String[] split = exchange.uri().split("/");
                            int customerId = Integer.parseInt(split[split.length - 3]);
                            int sellerId = Integer.parseInt(split[split.length - 2]);
                            int productId = Integer.parseInt(split[split.length - 1]);
                            CART_VMS.getTransactionManager().beginTransaction(0, 0, 0,false);
                            TransactionContext txCtx = ((TransactionManager) CART_VMS.getTransactionManager()).getTransactionContext();
                            CartItem cartItem = CART_REPO.lookupByKey( new CartItem.CartItemId(customerId, sellerId, productId) );
                            if(cartItem != null){
                                exchange.response().setChunked(true);
                                exchange.response().setStatusCode(200);
                                exchange.response().write(cartItem.toString());
                                exchange.response().end();
                            }
                            txCtx.close();
                        }
                        case "PATCH" -> {
                            String[] split = exchange.uri().split("/");
                            int customerId = Integer.parseInt(split[split.length - 2]);
                            String payload = buff.toString(StandardCharsets.UTF_8);
                            dk.ku.di.dms.vms.marketplace.common.entities.CartItem cartItemAPI =
                                    SERDES.deserialize(payload,
                                            dk.ku.di.dms.vms.marketplace.common.entities.CartItem.class);
                            long tid = CART_VMS.lastTidFinished();
                            CART_VMS.getTransactionManager().beginTransaction(tid, 0, Integer.MIN_VALUE, false);
                            CART_REPO.insert(CartUtils.convertCartItemAPI(customerId, cartItemAPI));
                            CART_VMS.getTransactionManager().commit();
                            exchange.response().setStatusCode(200).end();
                        }
                        default -> exchange.response().setStatusCode(500).end();
                    }
                } else {
                    exchange.response().setStatusCode(500).end();
                }

            });
        }
    }

}
