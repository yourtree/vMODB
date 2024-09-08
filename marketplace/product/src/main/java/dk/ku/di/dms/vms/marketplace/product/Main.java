package dk.ku.di.dms.vms.marketplace.product;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.product.infra.ProductDbUtils;
import dk.ku.di.dms.vms.marketplace.product.infra.ProductHttpServerVertx;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.CompositeKey;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ForkJoinPool;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;

public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    public static void main(String[] ignoredArgs) throws Exception {
        Properties properties = ConfigUtils.loadProperties();
        VmsApplication vms = initVms(properties);
        initHttpServer(properties, vms);
    }

    public static VmsApplication initVms(Properties properties) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                properties,
                "0.0.0.0",
                Constants.PRODUCT_VMS_PORT, new String[]{
                        "dk.ku.di.dms.vms.marketplace.product",
                        "dk.ku.di.dms.vms.marketplace.common"
                });
        VmsApplication vms = VmsApplication.build(options);
        vms.start();
        return vms;
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
            ProductHttpServerVertx.init(vms, numVertices, httpThreadPoolSize, nativeTransport);
            LOGGER.log(INFO,"Product: Vertx HTTP Server started");
            return;
        }
        if(httpServer.equalsIgnoreCase("jdk")){
            int backlog = Integer.parseInt( properties.getProperty("backlog") );
            initHttpServerJdk(vms, backlog);
            LOGGER.log(INFO,"Product: JDK HTTP Server started");
            return;
        }
        throw new RuntimeException("http_server property is unknown: "+ httpServer);
    }

    private static void initHttpServerJdk(VmsApplication vms, int backlog) throws IOException {
        // initialize HTTP server for data ingestion
        System.setProperty("sun.net.httpserver.nodelay","true");
        HttpServer httpServer = HttpServer.create(new InetSocketAddress("0.0.0.0", Constants.PRODUCT_HTTP_PORT), backlog);
        httpServer.createContext("/product", new ProductHttpHandler(vms));
        httpServer.setExecutor(ForkJoinPool.commonPool());
        httpServer.start();
    }

    private static class ProductHttpHandler implements HttpHandler {
        private final Table table;
        private final AbstractProxyRepository<Product.ProductId, Product> repository;
        private static final IVmsSerdesProxy SERDES = VmsSerdesProxyBuilder.build();

        @SuppressWarnings("unchecked")
        public ProductHttpHandler(VmsApplication vms){
            this.table = vms.getTable("products");
            this.repository = (AbstractProxyRepository<Product.ProductId, Product>) vms.getRepositoryProxy("products");
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                switch (exchange.getRequestMethod()) {
                    case "GET": {
                        String[] split = exchange.getRequestURI().toString().split("/");
                        int sellerId = Integer.parseInt(split[split.length - 2]);
                        int productId = Integer.parseInt(split[split.length - 1]);
                        Object[] obj = new Object[2];
                        obj[0] = sellerId;
                        obj[1] = productId;
                        IKey key = CompositeKey.of(obj);
                        // bypass transaction manager
                        Object[] record = this.table.underlyingPrimaryKeyIndex().lookupByKey(key);
                        /*
                        // the statement won't work because this is not part of transaction
                        // Product.ProductId id = new Product.ProductId(sellerId, productId);
                        // var entity = this.repository.lookupByKey(id);
                        */
                        try {
                            var entity = this.repository.parseObjectIntoEntity(record);
                            OutputStream outputStream = exchange.getResponseBody();
                            exchange.sendResponseHeaders(200, 0);
                            outputStream.write(entity.toString().getBytes(StandardCharsets.UTF_8));
                            outputStream.close();
                        } catch (RuntimeException e) {
                            OutputStream outputStream = exchange.getResponseBody();
                            exchange.sendResponseHeaders(404, 0);
                            outputStream.close();
                        }
                        break;
                    }
                    case "POST": {
                        String payload = new String(exchange.getRequestBody().readAllBytes());
                        LOGGER.log(DEBUG, "APP: POST request for product: \n" + payload);
                        ProductDbUtils.addProduct(payload, this.repository, this.table);
                        exchange.sendResponseHeaders(200, 0);
                        exchange.getResponseBody().close();
                        LOGGER.log(DEBUG, "APP: POST request succeeded for product: \n" + payload);
                        break;
                    }
                    default: {
                        failClose(exchange);
                    }
                }
            }
            catch (Exception e){
                failClose(exchange);
            }
        }

        private static void failClose(HttpExchange exchange ) throws IOException {
            // failed response
            OutputStream outputStream = exchange.getResponseBody();
            exchange.sendResponseHeaders(404, 0);
            outputStream.close();
        }

    }
}