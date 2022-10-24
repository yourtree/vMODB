package dk.ku.di.dms.vms.e_commerce;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class HttpServer {

    public final String context;

    public final HttpHandler handler;

    public HttpServer(String context, HttpHandler handler){
        this.context = context;
        this.handler = handler;
    }

    public void start() throws IOException {

        System.setProperty("sun.net.httpserver.nodelay","true");

        com.sun.net.httpserver.HttpServer server = com.sun.net.httpserver.HttpServer.
                create(new InetSocketAddress("localhost", 8001), 0);

        server.createContext(this.context, this.handler);

        server.setExecutor(Executors.newSingleThreadExecutor());

        server.start();

    }



}
