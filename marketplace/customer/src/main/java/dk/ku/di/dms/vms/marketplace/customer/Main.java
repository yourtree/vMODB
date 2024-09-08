package dk.ku.di.dms.vms.marketplace.customer;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.common.infra.HttpServerJdk;
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

public final class Main {

    public static void main(String[] args) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                "0.0.0.0",
                Constants.CUSTOMER_VMS_PORT,
                new String[]{
                "dk.ku.di.dms.vms.marketplace.customer",
                "dk.ku.di.dms.vms.marketplace.common"}
        );
        VmsApplication vms = VmsApplication.build(options);
        vms.start();
        HttpServerJdk.init(vms, "/customer", Constants.CUSTOMER_HTTP_PORT);
        System.out.println("Customer: JDK HTTP Server started");
    }

    private static class CustomerHttpHandler implements HttpHandler {
        private final Table table;
        private final AbstractProxyRepository<Integer, Customer> repository;
        VmsApplication vms;
        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        @SuppressWarnings("unchecked")
        public CustomerHttpHandler(VmsApplication vms){
            this.vms = vms;
            this.table = vms.getTable("customers");
            this.repository = (AbstractProxyRepository<Integer, Customer>) vms.getRepositoryProxy("customers");
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String str = new String( exchange.getRequestBody().readAllBytes() );
            Customer customer = this.serdes.deserialize(str, Customer.class);
            Object[] obj = this.repository.extractFieldValuesFromEntityObject(customer);
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
