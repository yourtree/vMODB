package dk.ku.di.dms.vms.marketplace.seller;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.seller.dtos.SellerDashboard;
import dk.ku.di.dms.vms.marketplace.seller.entities.Seller;
import dk.ku.di.dms.vms.marketplace.seller.infra.SellerHttpServerVertx;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.definition.key.SimpleKey;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static java.lang.System.Logger.Level.INFO;

public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                "localhost",
                Constants.SELLER_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.seller",
                "dk.ku.di.dms.vms.marketplace.common"
        });
        VmsApplication vms = VmsApplication.build(options);
        vms.start();
        // initHttpServerJdk(vms);
        initHttpServerVertx(vms);
        LOGGER.log(INFO, "Seller HTTP Server initialized");
    }

    private static void initHttpServerJdk(VmsApplication vms) throws IOException {
        // initialize HTTP server to serve seller dashboard online requests
        HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", 8007), 0);
        httpServer.createContext("/seller", new SellerHttpHandler(vms));
        httpServer.start();
    }

    private static void initHttpServerVertx(VmsApplication vms){
        SellerHttpServerVertx.init(vms, 4, true);
    }

    private static class SellerHttpHandler implements HttpHandler {

        private final Table sellerTable;

        private final AbstractProxyRepository<Integer, Seller> sellerRepository;

        private static final IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        private final SellerService sellerService;

        private final VmsApplication vms;

        @SuppressWarnings("unchecked")
        private SellerHttpHandler(VmsApplication vms) {
            this.vms = vms;
            var optService = vms.<SellerService>getService();
            if(optService.isEmpty()){
                throw new RuntimeException("Could not find service for SellerService");
            }
            this.sellerService = optService.get();
            this.sellerTable = vms.getTable("sellers");
            this.sellerRepository = (AbstractProxyRepository<Integer, Seller>) vms.getRepositoryProxy("sellers");
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            switch (exchange.getRequestMethod()){
                case "GET": {
                    //  seller dashboard. send fetch and fetchMany directly to transaction manager?
                    //  one way: the tx manager assigns the last finished tid to thread, thus obtaining the freshest snapshot possible
                    // another way: reentrant locks in the application code
                    String[] split = exchange.getRequestURI().toString().split("/");
                    if(split[2].contentEquals("dashboard")){
                        int sellerId = Integer.parseInt(split[split.length - 1]);
                        // register a transaction with the last tid finished
                        // this allows to get the freshest view, bypassing the scheduler
                        long lastTid = this.vms.lastTidFinished();
                        try(var txCtx = this.vms.getTransactionManager().beginTransaction(lastTid, 0, lastTid, true)) {
                            SellerDashboard view = this.sellerService.queryDashboard(sellerId);
                            // parse and return result
                            OutputStream outputStream = exchange.getResponseBody();
                            exchange.sendResponseHeaders(200, 0);
                            outputStream.write( view.toString().getBytes(StandardCharsets.UTF_8) );
                            outputStream.close();
                        }
                    } else {
                        int sellerId = Integer.parseInt(split[split.length - 1]);
                        IKey key = SimpleKey.of( sellerId );
                        // bypass transaction manager
                        Object[] record = this.sellerTable.underlyingPrimaryKeyIndex().lookupByKey(key);
                        var entity = this.sellerRepository.parseObjectIntoEntity(record);
                        OutputStream outputStream = exchange.getResponseBody();
                        exchange.sendResponseHeaders(200, 0);
                        outputStream.write( entity.toString().getBytes(StandardCharsets.UTF_8) );
                        outputStream.flush();
                        outputStream.close();
                        return;
                    }
                }
                case "POST": {
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
            }
            // failed response
            OutputStream outputStream = exchange.getResponseBody();
            exchange.sendResponseHeaders(404, 0);
            outputStream.flush();
            outputStream.close();
        }
    }

}
