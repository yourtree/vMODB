package dk.ku.di.dms.vms.marketplace.stock.infra;

import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.stock.StockItem;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;
import io.vertx.core.*;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

public final class StockHttpServerVertx extends AbstractVerticle {

    static VmsApplication STOCK_VMS;
    static AbstractProxyRepository<StockItem.StockId, StockItem> STOCK_REPO;

    @SuppressWarnings("unchecked")
    public static void init(VmsApplication stockVms, int numVertices, boolean nativeTransport){
        STOCK_VMS = stockVms;
        STOCK_REPO = (AbstractProxyRepository<StockItem.StockId, StockItem>) STOCK_VMS.getRepositoryProxy("stock_items");
        Vertx vertx = Vertx.vertx(new VertxOptions().setPreferNativeTransport(nativeTransport));
        boolean usingNative = vertx.isNativeTransportEnabled();
        System.out.println("Vertx is running with native: " + usingNative);
        DeploymentOptions deploymentOptions = new DeploymentOptions()
                .setThreadingModel(ThreadingModel.EVENT_LOOP)
                .setInstances(numVertices);

        try {
            vertx.deployVerticle(StockHttpServerVertx.class,
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
        options.setPort(Constants.STOCK_HTTP_PORT);
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
            final String[] uriSplit = exchange.uri().split("/");
            if (uriSplit.length < 2 || !uriSplit[1].equals("stock")) {
                handleError(exchange, "Invalid URI: "+exchange.uri());
                return;
            }
            exchange.bodyHandler(buff -> {
                switch (exchange.method().name()) {
                    case "GET" -> {
                        int sellerId = Integer.parseInt(uriSplit[uriSplit.length - 2]);
                        int productId = Integer.parseInt(uriSplit[uriSplit.length - 1]);
                        try(var txCtx = STOCK_VMS.getTransactionManager().beginTransaction(0, 0, 0,true)) {
                            StockItem stockItem = STOCK_REPO.lookupByKey(new StockItem.StockId(sellerId, productId));
                            if (stockItem == null) {
                                handleError(exchange, "Stock item does not exists");
                                return;
                            }
                            exchange.response().setChunked(true);
                            exchange.response().setStatusCode(200);
                            exchange.response().write(stockItem.toString());
                            exchange.response().end();
                        } catch (Exception e) {
                            handleError(exchange, e.getMessage());
                        }
                    }
                    case "PATCH" -> {
                        String op = uriSplit[uriSplit.length - 1];
                        if(op.contentEquals("reset")){
                            // path: /stock/reset
                            try {
                                STOCK_VMS.getTransactionManager().reset();
                                exchange.response().setStatusCode(200).end();
                            } catch (Exception e){
                                handleError(exchange, e.getMessage());
                            }
                            return;
                        }
                        try(var txCtx = STOCK_VMS.getTransactionManager().beginTransaction(0, 0, 0,false)){
                            var stockItems = STOCK_REPO.getAll();
                            for(StockItem item : stockItems){
                                item.qty_available = 10000;
                                item.version = "0";
                                item.qty_reserved = 0;
                                STOCK_REPO.upsert(item);
                            }
                            exchange.response().setStatusCode(200).end();
                        } catch (Exception e) {
                            handleError(exchange, e.getMessage());
                        }
                    }
                    case "POST" -> {
                        try {
                            String payload = buff.toString(StandardCharsets.UTF_8);
                            // StockDbUtils.addStockItem(payload, STOCK_REPO, STOCK_VMS.getTable("stock_items"));
                            StockItem stockItem = StockDbUtils.deserializeStockItem(payload);
                            try(var txCtx = STOCK_VMS.getTransactionManager().beginTransaction(0, 0, 0, false)) {
                                STOCK_REPO.upsert(stockItem);
                                exchange.response().setStatusCode(200).end();
                            } catch (Exception e){
                                handleError(exchange, e.getMessage());
                            }
                        } catch (Exception e) {
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
