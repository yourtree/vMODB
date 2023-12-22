package dk.ku.di.dms.vms.marketplace.product;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class Main {

//    private static final Logger logger = Logger.getLogger("VmsApplication");
    public static void main(String[] args) throws Exception {

        // initialize threads
        VmsApplication vms = VmsApplication.build("localhost", 8081, new String[]{
                "dk.ku.di.dms.vms.marketplace.product",
                "dk.ku.di.dms.vms.marketplace.common"
                        });
        vms.start();

        // initialize HTTP server for data ingestion
        HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", 8001), 0);
        httpServer.createContext("/product", new ProductHttpHandler(vms));
        httpServer.start();
    }

    private static class ProductHttpHandler implements HttpHandler {
        private final Table table;
        VmsApplication vms;
        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();
        public ProductHttpHandler(VmsApplication vms){
            this.vms = vms;
            this.table = vms.getTable("products");
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String str = new String( exchange.getRequestBody().readAllBytes() );
            Product product = serdes.deserialize(str, Product.class);
            Object[] obj = convert(product);
            IKey key = KeyUtils.buildPrimaryKey( table.getSchema(), obj );
            table.underlyingPrimaryKeyIndex_().insert(key, obj);

            // response
            OutputStream outputStream = exchange.getResponseBody();
            exchange.sendResponseHeaders(200, 0);
            outputStream.flush();
            outputStream.close();
        }

        private static Object[] convert(Product product){
            Object[] obj = new Object[10];
            obj[0] = product.seller_id;
            obj[1] = product.product_id;
            obj[2] = product.name;
            obj[3] = product.sku;
            obj[4] = product.category;
            obj[5] = product.description;
            obj[6] = product.price;
            obj[7] = product.freight_value;
            obj[8] = product.status;
            obj[9] = product.version;
            return obj;
        }
    }
}