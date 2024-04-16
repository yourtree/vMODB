package dk.ku.di.dms.vms.marketplace.product;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import static dk.ku.di.dms.vms.marketplace.common.Constants.PRODUCT_HTTP_PORT;

public final class Main {

    public static void main(String[] args) {

        // initialize threads
        try {
            VmsApplication vms = VmsApplication.build("localhost", Constants.PRODUCT_VMS_PORT, new String[]{
                    "dk.ku.di.dms.vms.marketplace.product",
                    "dk.ku.di.dms.vms.marketplace.common"
            });
            vms.start();

            // initialize HTTP server for data ingestion
            HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", PRODUCT_HTTP_PORT), 0);
            httpServer.createContext("/product", new ProductHttpHandler(vms));
            httpServer.start();
        } catch (Exception e){
            throw new RuntimeException(e);
        }

    }

    private static class ProductHttpHandler implements HttpHandler {
        private final Table table;
        private final AbstractProxyRepository<Product.ProductId, Product> repository;
        private static final IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        @SuppressWarnings("unchecked")
        public ProductHttpHandler(VmsApplication vms){
            this.table = vms.getTable("products");
            this.repository = (AbstractProxyRepository<Product.ProductId, Product>) vms.getRepositoryProxy("products");
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String str = new String( exchange.getRequestBody().readAllBytes() );
            Product product = serdes.deserialize(str, Product.class);

            Object[] obj = this.repository.extractFieldValuesFromEntityObject(product);
            IKey key = KeyUtils.buildRecordKey( table.schema().getPrimaryKeyColumns(), obj );
            this.table.underlyingPrimaryKeyIndex().insert(key, obj);

            // response
            OutputStream outputStream = exchange.getResponseBody();
            exchange.sendResponseHeaders(200, 0);
            outputStream.flush();
            outputStream.close();
        }
    }
}