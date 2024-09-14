package dk.ku.di.dms.vms.marketplace.cart.infra;

import dk.ku.di.dms.vms.marketplace.cart.repositories.ICartItemRepository;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;

public final class CartHttpServerVertx { //extends AbstractVerticle {

    static VmsApplication CART_VMS;
    static ICartItemRepository CART_REPO;
    static IVmsSerdesProxy SERDES = VmsSerdesProxyBuilder.build();

    public static void init(VmsApplication cartVms, int numVertices, int httpThreadPoolSize, boolean nativeTransport){
        CART_VMS = cartVms;
        CART_REPO = (ICartItemRepository) CART_VMS.getRepositoryProxy("cart_items");
//        VertxOptions vertxOptions = new VertxOptions().setPreferNativeTransport(nativeTransport);
//        if(httpThreadPoolSize > 0){
//            vertxOptions.setEventLoopPoolSize(httpThreadPoolSize);
//        }
//        Vertx vertx = Vertx.vertx(vertxOptions);
//        boolean usingNative = vertx.isNativeTransportEnabled();
//        DeploymentOptions deploymentOptions = new DeploymentOptions()
//                .setThreadingModel(ThreadingModel.EVENT_LOOP)
//                .setInstances(numVertices);
//        try {
//            vertx.deployVerticle(CartHttpServerVertx.class,
//                            deploymentOptions
//                    )
//                    .toCompletionStage()
//                    .toCompletableFuture()
//                    .get();
//        } catch (InterruptedException | ExecutionException e) {
//            e.printStackTrace(System.out);
//            throw new RuntimeException(e);
//        }
//        System.out.println("Vertx is running with the following options:\n" +
//                "vertices: "+ numVertices +
//                "\nevent loop size: "+httpThreadPoolSize +
//                "\nnative: " + usingNative);
    }

    /*
    @Override
    public void start(Promise<Void> startPromise) {
        HttpServerOptions options = new HttpServerOptions();
        options.setPort(Constants.CART_HTTP_PORT);
        options.setHost("0.0.0.0");
        options.setTcpKeepAlive(true);
        options.setTcpNoDelay(true);
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        router.get("/cart/:customerId").handler(ctx -> {
            int customerId = Integer.parseInt(ctx.pathParam("customerId"));
            long tid = CART_VMS.lastTidFinished();
            try(var txCtx = CART_VMS.getTransactionManager().beginTransaction(tid, 0, 0,true)) {
                List<CartItem> cartItems = CART_REPO.getCartItemsByCustomerId(customerId);
                ctx.response()
                        .putHeader("content-type", "application/json")
                        .setStatusCode(200)
                        .end(cartItems.toString());
            } catch (Exception e) {
                ctx.response().putHeader("content-type", "text/plain")
                        .setStatusCode(500).end(e.getMessage());
            }
        });
        router.patch("/cart/reset").handler(ctx -> {
            CART_VMS.getTransactionManager().reset();
            ctx.response().setStatusCode(204).end();
        });
        router.patch("/cart/:customerId/add").handler(ctx -> {
            int customerId = Integer.parseInt(ctx.pathParam("customerId"));
            var payload = ctx.body().asString();
            dk.ku.di.dms.vms.marketplace.common.entities.CartItem cartItemAPI =
                    SERDES.deserialize(payload, dk.ku.di.dms.vms.marketplace.common.entities.CartItem.class);
            CART_ITEMS.computeIfAbsent(customerId, (x) -> new ArrayList<>()).add(
                    CartUtils.convertCartItemAPI(customerId, cartItemAPI)
            );
            ctx.response().setStatusCode(204).end();
        });
        HttpServer server = this.vertx.createHttpServer(options).requestHandler(router);
        server.listen(res -> {
            if (res.succeeded()) {
                startPromise.complete();
            } else {
                startPromise.fail(res.cause());
            }
        });
    }
*/
}
