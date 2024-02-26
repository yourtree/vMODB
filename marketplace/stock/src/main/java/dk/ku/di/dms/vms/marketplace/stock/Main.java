package dk.ku.di.dms.vms.marketplace.stock;

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

public class Main {
    public static void main(String[] args) throws Exception {
        VmsApplication vms = VmsApplication.build("localhost", Constants.STOCK_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.stock",
                "dk.ku.di.dms.vms.marketplace.common"
        });
        vms.start();

        HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", 8002), 0);
        httpServer.createContext("/stock", new StockHttpHandler(vms));
        httpServer.start();
    }

    private static class StockHttpHandler implements HttpHandler {
        private final Table table;
        private final AbstractProxyRepository<StockItem.StockId, StockItem> repository;
        VmsApplication vms;
        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        @SuppressWarnings("unchecked")
        public StockHttpHandler(VmsApplication vms){
            this.vms = vms;
            this.table = vms.getTable("stock_items");
            this.repository = (AbstractProxyRepository<StockItem.StockId, StockItem>) vms.getRepositoryProxy("stock_items");
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String str = new String( exchange.getRequestBody().readAllBytes() );
            StockItem stock = this.serdes.deserialize(str, StockItem.class);
            Object[] obj = this.repository.extractFieldValuesFromEntityObject(stock);
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