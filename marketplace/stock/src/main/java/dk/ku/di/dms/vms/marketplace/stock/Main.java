package dk.ku.di.dms.vms.marketplace.stock;

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

import static java.lang.System.Logger.Level.*;

public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {

        VmsApplicationOptions options = VmsApplicationOptions.build(
                "localhost",
                Constants.STOCK_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.stock",
                "dk.ku.di.dms.vms.marketplace.common"
        });

        VmsApplication vms = VmsApplication.build(options);
        vms.start();

        HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", Constants.STOCK_HTTP_PORT), 0);
        httpServer.createContext("/stock", new StockHttpHandler(vms));
        httpServer.start();
    }

    private static class StockHttpHandler implements HttpHandler {
        private final Table table;
        private final AbstractProxyRepository<StockItem.StockId, StockItem> repository;
        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        @SuppressWarnings("unchecked")
        public StockHttpHandler(VmsApplication vms){
            this.table = vms.getTable("stock_items");
            this.repository = (AbstractProxyRepository<StockItem.StockId, StockItem>) vms.getRepositoryProxy("stock_items");
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            switch (exchange.getRequestMethod()) {
                case "GET": {
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
                    break;
                }
                case "POST": {
                    String str = new String( exchange.getRequestBody().readAllBytes() );

                    LOGGER.log(DEBUG, "APP: POST request for stock item: \n" + str);

                    StockItem stock = this.serdes.deserialize(str, StockItem.class);
                    Object[] obj = this.repository.extractFieldValuesFromEntityObject(stock);
                    IKey key = KeyUtils.buildRecordKey( table.schema().getPrimaryKeyColumns(), obj );
                    this.table.underlyingPrimaryKeyIndex().insert(key, obj);

                    // response
                    OutputStream outputStream = exchange.getResponseBody();
                    exchange.sendResponseHeaders(200, 0);
                    outputStream.flush();
                    outputStream.close();
                    break;
                }
                default: {
                    // failed response
                    OutputStream outputStream = exchange.getResponseBody();
                    exchange.sendResponseHeaders(404, 0);
                    outputStream.flush();
                    outputStream.close();
                }
            }
        }
    }

}