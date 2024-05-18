package dk.ku.di.dms.vms.marketplace.seller;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.common.Utils;
import dk.ku.di.dms.vms.marketplace.seller.dtos.OrderSellerView;
import dk.ku.di.dms.vms.marketplace.seller.entities.Seller;
import dk.ku.di.dms.vms.marketplace.seller.repositories.IOrderEntryRepository;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
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
import java.util.Properties;

public final class Main {

    public static void main(String[] args) throws Exception {

        Properties properties = Utils.loadProperties();
        int networkBufferSize = Integer.parseInt( properties.getProperty("network_buffer_size") );
        int networkThreadPoolSize = Integer.parseInt( properties.getProperty("network_thread_pool_size") );
        int networkSendTimeout = Integer.parseInt( properties.getProperty("network_send_timeout") );

        VmsApplicationOptions options = new VmsApplicationOptions("localhost", Constants.SELLER_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.seller",
                "dk.ku.di.dms.vms.marketplace.common"
        }, networkBufferSize == 0 ? MemoryUtils.DEFAULT_PAGE_SIZE : networkBufferSize,
                networkThreadPoolSize, networkSendTimeout);

        VmsApplication vms = VmsApplication.build(options);
        vms.start();

        // initialize HTTP server to serve seller dashboard online requests
        HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", 8007), 0);
        httpServer.createContext("/seller", new SellerHttpHandler(vms));
        httpServer.start();

        System.out.println("Seller HTTP Server initialized");
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

            switch (exchange.getRequestMethod()){
                case "GET": {
                    //  seller dashboard. send fetch and fetchMany directly to transaction manager?
                    //  one way: the tx manager assigns the last finished tid to thread, thus obtaining the freshest snapshot possible
                    // another way: reentrant locks in the application code
                    String[] split = exchange.getRequestURI().toString().split("/");

                    if(split[2].contentEquals("dashboard")){
                        // register a transaction with the last tid finished to get the freshest view
                        // not necessary. the concurrent hashmap guarantees all-or-nothing
                        // vms.lastTidFinished()

                        int sellerId = Integer.parseInt(split[split.length - 1]);
                        OrderSellerView view = this.sellerService.queryDashboard(sellerId);
                        // parse and return result
                        OutputStream outputStream = exchange.getResponseBody();
                        exchange.sendResponseHeaders(200, 0);
                        outputStream.write( view.toString().getBytes(StandardCharsets.UTF_8) );
                        outputStream.close();
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
                    }
                    return;
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
