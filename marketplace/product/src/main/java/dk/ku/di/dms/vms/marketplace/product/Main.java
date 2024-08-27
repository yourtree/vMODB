package dk.ku.di.dms.vms.marketplace.product;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.CompositeKey;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.concurrent.ForkJoinPool;

import static dk.ku.di.dms.vms.marketplace.common.Constants.PRODUCT_HTTP_PORT;
import static java.lang.System.Logger.Level.INFO;

public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    public static VmsApplication init(){
        VmsApplicationOptions options = VmsApplicationOptions.build(
                "0.0.0.0",
                Constants.PRODUCT_VMS_PORT, new String[]{
                        "dk.ku.di.dms.vms.marketplace.product",
                        "dk.ku.di.dms.vms.marketplace.common"
                });
        // initialize threads
        try {
            VmsApplication vms = VmsApplication.build(options);
            vms.start();
            // initialize HTTP server for data ingestion
            System.setProperty("sun.net.httpserver.nodelay","true");
            HttpServer httpServer = HttpServer.create(new InetSocketAddress("0.0.0.0", PRODUCT_HTTP_PORT), 0);
            httpServer.createContext("/product", new ProductHttpHandler(vms));
            httpServer.setExecutor(ForkJoinPool.commonPool());
            httpServer.start();
            LOGGER.log(INFO,"Product HTTP Server initialized");
            return vms;
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] ignoredArgs) {
        init();
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
                        String str = new String(exchange.getRequestBody().readAllBytes());
                        LOGGER.log(INFO, "APP: POST request for product: \n" + str);
                        Product product = SERDES.deserialize(str, Product.class);
                        Object[] obj = this.repository.extractFieldValuesFromEntityObject(product);
                        IKey key = KeyUtils.buildRecordKey(this.table.schema().getPrimaryKeyColumns(), obj);

                        // created and update at
                        Date dt = new Date();
                        obj[obj.length - 1] = dt;
                        obj[obj.length - 2] = dt;
                        this.table.underlyingPrimaryKeyIndex().insert(key, obj);

                        exchange.sendResponseHeaders(200, 0);
                        exchange.getResponseBody().close();

                        LOGGER.log(INFO, "APP: POST request suceeded for product: \n" + str);
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