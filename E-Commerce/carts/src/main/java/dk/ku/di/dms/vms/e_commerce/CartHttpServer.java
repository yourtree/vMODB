package dk.ku.di.dms.vms.e_commerce;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import dk.ku.di.dms.vms.e_commerce.cart.Cart;

import java.io.IOException;

public class CartHttpServer implements HttpHandler {

    public final HttpServer httpServer;

    public CartHttpServer(){
        this.httpServer = new HttpServer("/carts", this);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {

        // TODO finish exchange.getRequestBody().read()

    }

}
