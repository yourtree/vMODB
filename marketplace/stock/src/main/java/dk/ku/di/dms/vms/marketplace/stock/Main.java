package dk.ku.di.dms.vms.marketplace.stock;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.stock.infra.StockDbUtils;
import dk.ku.di.dms.vms.marketplace.stock.infra.StockHttpServerVertx;
import dk.ku.di.dms.vms.modb.api.interfaces.IHttpHandler;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.CompositeKey;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.System.Logger;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ForkJoinPool;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;

public final class Main {

    private static final Logger LOGGER = System.getLogger(Main.class.getName());

    public static void main(String[] ignoredArgs) throws Exception {
        Properties properties = ConfigUtils.loadProperties();
        VmsApplication vms = initVms(properties);
        // initHttpServer(properties, vms);
    }

    private static VmsApplication initVms(Properties properties) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                properties,
                "0.0.0.0",
                Constants.STOCK_VMS_PORT, new String[]{
                        "dk.ku.di.dms.vms.marketplace.stock",
                        "dk.ku.di.dms.vms.marketplace.common"
                });
        VmsApplication vms = VmsApplication.build(options,
                (x,y) -> new StockHttpHandlerJdk2(x, (IStockRepository) y.apply("stock_items")));
        vms.start();
        return vms;
    }
    
    private static class StockHttpHandlerJdk2 implements IHttpHandler {

        private final ITransactionManager transactionManager;
        private final IStockRepository repository;

        public StockHttpHandlerJdk2(ITransactionManager transactionManager,
                                     IStockRepository repository){
            this.transactionManager = transactionManager;
            this.repository = repository;
        }

        @Override
        public void post(String uri, String body) throws Exception {
            StockItem stockItem = StockDbUtils.deserializeStockItem(body);
            try(var txCtx = this.transactionManager.beginTransaction(0, 0, 0, false)) {
                this.repository.upsert(stockItem);
            }
        }

        @Override
        public void patch(String uri, String body) throws Exception {
            final String[] uriSplit = uri.split("/");
            String op = uriSplit[uriSplit.length - 1];
            if(op.contentEquals("reset")){
                // path: /stock/reset
                this.transactionManager.reset();
                return;
            }
            try(var txCtx = this.transactionManager.beginTransaction(0, 0, 0,false)){
                List<StockItem> stockItems = this.repository.getAll();
                for(StockItem item : stockItems){
                    item.qty_available = 10000;
                    item.version = "0";
                    item.qty_reserved = 0;
                    this.repository.upsert(item);
                }
            }
        }

        @Override
        public String get(String uri) throws Exception {
            final String[] uriSplit = uri.split("/");
            int sellerId = Integer.parseInt(uriSplit[uriSplit.length - 2]);
            int productId = Integer.parseInt(uriSplit[uriSplit.length - 1]);
            try (var txCtx = this.transactionManager.beginTransaction(0, 0, 0, true)) {
                StockItem stockItem = this.repository.lookupByKey(new StockItem.StockId(sellerId, productId));
                return stockItem.toString();
            }
        }
    }

    private static void initHttpServer(Properties properties, VmsApplication vms) throws IOException {
        String httpServer = properties.getProperty("http_server");
        if(httpServer == null || httpServer.isEmpty()){
            throw new RuntimeException("http_server property is missing");
        }
        if(httpServer.equalsIgnoreCase("vertx")){
            int httpThreadPoolSize = Integer.parseInt( properties.getProperty("http_thread_pool_size") );
            int numVertices = Integer.parseInt( properties.getProperty("num_vertices") );
            boolean nativeTransport = Boolean.parseBoolean( properties.getProperty("native_transport") );
            StockHttpServerVertx.init(vms, numVertices, httpThreadPoolSize, nativeTransport);
            LOGGER.log(INFO,"Stock: Vertx HTTP Server started");
            return;
        }
        if(httpServer.equalsIgnoreCase("jdk")){
            int backlog = Integer.parseInt( properties.getProperty("backlog") );
            initHttpServerJdk(vms, backlog);
            LOGGER.log(INFO,"Stock: JDK HTTP Server started");
            return;
        }
        throw new RuntimeException("http_server property is unknown: "+ httpServer);
    }

    private static void initHttpServerJdk(VmsApplication vms, int backlog) throws IOException {
        // initialize HTTP server for data ingestion
        System.setProperty("sun.net.httpserver.nodelay","true");
        HttpServer httpServer = HttpServer.create(new InetSocketAddress("0.0.0.0", Constants.STOCK_HTTP_PORT), backlog);
        httpServer.createContext("/stock", new StockHttpHandler(vms));
        httpServer.setExecutor(ForkJoinPool.commonPool());
        httpServer.start();
    }

    private static class StockHttpHandler implements HttpHandler {
        private final Table table;
        private final AbstractProxyRepository<StockItem.StockId, StockItem> repository;

        @SuppressWarnings("unchecked")
        public StockHttpHandler(VmsApplication vms){
            this.table = vms.getTable("stock_items");
            this.repository = (AbstractProxyRepository<StockItem.StockId, StockItem>) vms.getRepositoryProxy("stock_items");
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            switch (exchange.getRequestMethod()) {
                case "GET" -> {
                    String[] split = exchange.getRequestURI().toString().split("/");
                    int sellerId = Integer.parseInt(split[split.length - 2]);
                    int productId = Integer.parseInt(split[split.length - 1]);

                    Object[] obj = new Object[2];
                    obj[0] = sellerId;
                    obj[1] = productId;
                    IKey key = CompositeKey.of( obj );
                    // bypass transaction manager
                    Object[] record = this.table.underlyingPrimaryKeyIndex().lookupByKey(key);

                    try{
                        var entity = this.repository.parseObjectIntoEntity(record);
                        OutputStream outputStream = exchange.getResponseBody();
                        exchange.sendResponseHeaders(200, 0);
                        outputStream.write( entity.toString().getBytes(StandardCharsets.UTF_8) );
                        outputStream.close();
                    } catch(RuntimeException e) {
                        OutputStream outputStream = exchange.getResponseBody();
                        exchange.sendResponseHeaders(404, 0);
                        outputStream.flush();
                        outputStream.close();
                    }
                }
                case "POST" -> {
                    String payload = new String( exchange.getRequestBody().readAllBytes() );
                    LOGGER.log(DEBUG, "APP: POST request for stock item: \n" + payload);
                    StockDbUtils.addStockItem(payload, this.repository, this.table);
                    // response
                    OutputStream outputStream = exchange.getResponseBody();
                    exchange.sendResponseHeaders(200, 0);
                    outputStream.flush();
                    outputStream.close();
                }
                default -> {
                    // failed response
                    OutputStream outputStream = exchange.getResponseBody();
                    exchange.sendResponseHeaders(404, 0);
                    outputStream.close();
                }
            }
        }
    }

}