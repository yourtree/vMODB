package dk.ku.di.dms.vms.marketplace.seller;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;

import java.io.IOException;
import java.net.InetSocketAddress;

public final class Main {

    public static void main(String[] args) throws Exception {
        VmsApplication vms = VmsApplication.build("localhost", 8087, new String[]{
                "dk.ku.di.dms.vms.marketplace.seller",
                "dk.ku.di.dms.vms.marketplace.common"
        });
        vms.start();

        // initialize HTTP server to serve seller dashboard online requests
        HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", 8007), 0);
        httpServer.createContext("/product", new SellerHttpHandler());
        httpServer.start();
    }

    private static class SellerHttpHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // TODO finish
        }
    }

}
