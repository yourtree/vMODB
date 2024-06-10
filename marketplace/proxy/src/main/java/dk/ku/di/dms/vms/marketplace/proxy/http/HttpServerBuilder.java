package dk.ku.di.dms.vms.marketplace.proxy.http;

import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.Coordinator;
import dk.ku.di.dms.vms.marketplace.proxy.Main;

import java.io.IOException;
import java.util.Properties;

import static java.lang.System.Logger.Level.INFO;

public final class HttpServerBuilder {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    public static void build(Properties properties, Coordinator coordinator) throws IOException {
        String httpServer = properties.getProperty("http_server");
        if(httpServer == null || httpServer.isEmpty()){
            throw new RuntimeException("http_server property is missing");
        }
        if(httpServer.equalsIgnoreCase("vertx")){
            buildVertx(properties, coordinator);
            LOGGER.log(INFO,"Proxy: Vertx HTTP Server started");
            return;
        }
        if(httpServer.equalsIgnoreCase("jdk")){
            buildJdk(properties, coordinator);
            LOGGER.log(INFO,"Proxy: JDK HTTP Server started");
            return;
        }
        throw new RuntimeException("http_server property is unknown: "+ httpServer);
    }

    private static void buildVertx(Properties properties, Coordinator coordinator){
        HttpServerVertx.init(properties, coordinator);
    }

    private static void buildJdk(Properties properties, Coordinator coordinator) throws IOException {
        HttpServerJdk.init(properties, coordinator);
    }

}
