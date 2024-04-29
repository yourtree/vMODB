package dk.ku.di.dms.vms.marketplace.seller;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.seller.entities.Seller;
import dk.ku.di.dms.vms.marketplace.seller.repositories.IOrderEntryRepository;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public final class Main {

    public static void main(String[] args) throws Exception {

        VmsApplicationOptions options = new VmsApplicationOptions("localhost", Constants.SELLER_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.seller",
                "dk.ku.di.dms.vms.marketplace.common"
        }, 4096, 2);

        VmsApplication vms = VmsApplication.build(options);
        vms.start();

        // initialize HTTP server to serve seller dashboard online requests
        HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", 8007), 0);
        httpServer.createContext("/seller", new SellerHttpHandler(vms));
        httpServer.start();
    }

    private static class SellerHttpHandler implements HttpHandler {

        private final IOrderEntryRepository orderEntryRepository;

        private final Table sellerTable;

        private final AbstractProxyRepository<Integer, Seller> sellerRepository;

        private static final IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        private final SellerService sellerService;

        @SuppressWarnings("unchecked")
        private SellerHttpHandler(VmsApplication vms) {
            this.sellerService = ((SellerService) vms.getService());
            this.orderEntryRepository = (IOrderEntryRepository) vms.getRepositoryProxy("packages");
            this.sellerTable = vms.getTable("sellers");
            this.sellerRepository = (AbstractProxyRepository<Integer, Seller>) vms.getRepositoryProxy("sellers");
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if(exchange.getRequestMethod().contentEquals("POST")){
                String str = new String( exchange.getRequestBody().readAllBytes() );
                Seller seller = serdes.deserialize(str, Seller.class);

                Object[] obj = this.sellerRepository.extractFieldValuesFromEntityObject(seller);
                IKey key = KeyUtils.buildRecordKey( sellerTable.schema().getPrimaryKeyColumns(), obj );
                this.sellerTable.underlyingPrimaryKeyIndex().insert(key, obj);

                // response
                OutputStream outputStream = exchange.getResponseBody();
                exchange.sendResponseHeaders(200, 0);
                outputStream.flush();
                outputStream.close();
                return;
            }

            // TODO seller dashboard. send fetch and fetchMany directly to transaction manager?
            //  the tx manager assigns the last finished tid to thread, thus obtaining the freshest snapshot possible
            if(exchange.getRequestURI().getPath().contentEquals("dashboard")){
                // register a transaction with the last tid finished to get the freshest view
                // not necessary. the concurrent hashmap guarantees all-or-nothing
                // vms.lastTidFinished()
                // sellerService.queryDashboard();
            }

        }
    }

}
