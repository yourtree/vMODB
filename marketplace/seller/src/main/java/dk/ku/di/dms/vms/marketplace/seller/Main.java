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
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
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

import static java.lang.System.Logger.Level.INFO;

public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {
        Properties properties = ConfigUtils.loadProperties();
        VmsApplication vms = initVms(properties);
        initHttpServer(properties, vms);
    }

    private static VmsApplication initVms(Properties properties) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                properties,
                "0.0.0.0",
                Constants.SELLER_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.seller",
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
            int numVertices = Integer.parseInt( properties.getProperty("num_vertices") );
            boolean nativeTransport = Boolean.parseBoolean( properties.getProperty("native_transport") );
            initHttpServerVertx(vms, numVertices, nativeTransport);
            LOGGER.log(INFO,"Seller: Vertx HTTP Server started");
            return;
        }
        if(httpServer.equalsIgnoreCase("jdk")){
            int backlog = Integer.parseInt( properties.getProperty("backlog") );
            initHttpServerJdk(vms, backlog);
            LOGGER.log(INFO,"Seller: JDK HTTP Server started");
            return;
        }
        throw new RuntimeException("http_server property is unknown: "+ httpServer);
    }

    private static void initHttpServerJdk(VmsApplication vms, int backlog) throws IOException {
        // initialize HTTP server to serve seller dashboard online requests
        System.setProperty("sun.net.httpserver.nodelay","true");
        HttpServer httpServer = HttpServer.create(new InetSocketAddress("0.0.0.0", Constants.SELLER_HTTP_PORT), backlog);
        httpServer.createContext("/seller", new SellerHttpHandler(vms));
        httpServer.start();
    }

    private static void initHttpServerVertx(VmsApplication vms, int numVertices, boolean nativeTransport){
        SellerHttpServerVertx.init(vms, numVertices, nativeTransport);
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
                        try(var _ = this.vms.getTransactionManager().beginTransaction(lastTid, 0, lastTid, true)) {
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
