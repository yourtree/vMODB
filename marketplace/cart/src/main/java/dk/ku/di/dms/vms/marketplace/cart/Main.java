package dk.ku.di.dms.vms.marketplace.cart;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.marketplace.cart.entities.CartItem;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.common.Utils;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.CompositeKey;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.NonUniqueSecondaryIndex;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public final class Main {

    public static void main(String[] ignoredArgs) {
        Properties properties = Utils.loadProperties();
        int networkBufferSize = Integer.parseInt(properties.getProperty("network_buffer_size"));
        int networkThreadPoolSize = Integer.parseInt(properties.getProperty("network_thread_pool_size"));

        VmsApplicationOptions options = new VmsApplicationOptions("localhost", Constants.CART_VMS_PORT, new String[]{
                "dk.ku.di.dms.vms.marketplace.cart",
                "dk.ku.di.dms.vms.marketplace.common"
        }, networkBufferSize == 0 ? MemoryUtils.DEFAULT_PAGE_SIZE : networkBufferSize,
                networkThreadPoolSize);

        try
        {
            VmsApplication vms = VmsApplication.build(options);
            vms.start();

            // initialize HTTP server for data ingestion
            HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", Constants.CART_HTTP_PORT), 0);
            httpServer.createContext("/cart", new CartHttpHandler(vms));
            httpServer.start();

            System.out.println("HTTP Server initialized");
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class CartHttpHandler implements HttpHandler {

        private final Table table;
        private final AbstractProxyRepository<CartItem.CartItemId, CartItem> repository;
        private static final IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();
        private final NonUniqueSecondaryIndex customerIdx;

        @SuppressWarnings("unchecked")
        public CartHttpHandler(VmsApplication vms){
            this.table = vms.getTable("cart_items");
            this.repository = (AbstractProxyRepository<CartItem.CartItemId, CartItem>) vms.getRepositoryProxy("cart_items");
            this.customerIdx = table.secondaryIndexMap.get( KeyUtils.buildIndexKey( new int[]{2} ) );
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            switch (exchange.getRequestMethod()) {
                case "GET": {
                    String[] split = exchange.getRequestURI().toString().split("/");
                    int customerId = Integer.parseInt(split[split.length - 3]);
                    int sellerId = Integer.parseInt(split[split.length - 2]);
                    int productId = Integer.parseInt(split[split.length - 1]);

                    Object[] obj = new Object[3];
                    obj[0] = sellerId;
                    obj[1] = productId;
                    obj[2] = customerId;

                    IKey key = CompositeKey.of( obj );
                    Object[] record = this.table.underlyingPrimaryKeyIndex().lookupByKey(key);

                    try {
                        var entity = this.repository.parseObjectIntoEntity(record);
                        OutputStream outputStream = exchange.getResponseBody();
                        exchange.sendResponseHeaders(200, 0);
                        outputStream.write( entity.toString().getBytes(StandardCharsets.UTF_8) );
                        outputStream.close();
                    } catch(RuntimeException e) {
                        returnFailed(exchange);
                    }
                    break;
                }
                case "PATCH": {
                    String[] split = exchange.getRequestURI().toString().split("/");

                    try {
                        int customerId = Integer.parseInt(split[split.length - 2]);

                        String str = new String(exchange.getRequestBody().readAllBytes());
                        dk.ku.di.dms.vms.marketplace.common.entities.CartItem cartItemAPI =
                                serdes.deserialize(str, dk.ku.di.dms.vms.marketplace.common.entities.CartItem.class);

                    /*
                    CartItem cartItem = new CartItem(
                            cartItemAPI.SellerId,
                            cartItemAPI.ProductId,
                            0, // customer id is given in the api
                            cartItemAPI.ProductName,
                            cartItemAPI.UnitPrice,
                            cartItemAPI.FreightValue,
                            cartItemAPI.Quantity,
                            cartItemAPI.Voucher,
                            cartItemAPI.Version
                            );

                    Object[] obj = this.repository.extractFieldValuesFromEntityObject(cartItem);
                    // put customer id on the correct position
                    obj[2] = customerId;
                    */
                        // bypass repository. use api object directly to transform payload
                        Object[] obj = new Object[]{
                                cartItemAPI.SellerId,
                                cartItemAPI.ProductId,
                                customerId,
                                cartItemAPI.ProductName,
                                cartItemAPI.UnitPrice,
                                cartItemAPI.FreightValue,
                                cartItemAPI.Quantity,
                                cartItemAPI.Voucher,
                                cartItemAPI.Version
                        };

                        IKey key = KeyUtils.buildRecordKey(table.schema().getPrimaryKeyColumns(), obj);
                        this.table.underlyingPrimaryKeyIndex().insert(key, obj);

                        // add to customer idx for fast lookup on checkout
                        this.customerIdx.insert(key, obj);

                        // response
                        OutputStream outputStream = exchange.getResponseBody();
                        exchange.sendResponseHeaders(200, 0);
                        outputStream.flush();
                        outputStream.close();

                    } catch(Exception e){
                        returnFailed(exchange);
                    }
                    break;
                }
                default : {
                    returnFailed(exchange);
                }
            }
        }

        private static void returnFailed(HttpExchange exchange) throws IOException {
            // failed response
            OutputStream outputStream = exchange.getResponseBody();
            exchange.sendResponseHeaders(404, 0);
            outputStream.flush();
            outputStream.close();
        }

    }

}
