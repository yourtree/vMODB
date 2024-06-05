package dk.ku.di.dms.vms.marketplace.proxy.http;

import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.Coordinator;
import dk.ku.di.dms.vms.marketplace.proxy.Main;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static java.lang.System.Logger.Level.INFO;

public final class HttpServerBuilder {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    public static void build(Properties properties, Coordinator coordinator) throws IOException {
        String httpServer = properties.getProperty("http_server");
        if(httpServer == null || httpServer.isEmpty()){
            throw new RuntimeException("http_server property is missing");
        }
        if(httpServer.equalsIgnoreCase("vertx")){
            LOGGER.log(INFO,"Proxy: Initializing Vertx HTTP Server to receive transaction inputs...");
            buildVertx(properties, coordinator);
            return;
        }
        if(httpServer.equalsIgnoreCase("jdk")){
            LOGGER.log(INFO,"Proxy: Initializing JDK9 HTTP Server to receive transaction inputs...");
            buildJdk(properties, coordinator);
            return;
        }
        throw new RuntimeException("http_server property is unknown: "+ httpServer);
    }

    private static void buildVertx(Properties properties, Coordinator coordinator){
        try {
            HttpServerVertx.init(properties, coordinator);
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void buildJdk(Properties properties, Coordinator coordinator) throws IOException {
        HttpServerJdk.init(properties, coordinator);
    }

}
