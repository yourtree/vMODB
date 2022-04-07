package dk.ku.di.dms.vms.sdk.core.client.websocket;

import dk.ku.di.dms.vms.sdk.core.event.handler.IVmsEventHandler;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadata;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;

public class WebSocketHandlerBuilder {

    public static IVmsEventHandler build(final ExecutorService executor, final VmsMetadata vmsMetadata){
        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .executor(executor)
                .build();
        return buildActualWebSocketClient(httpClient, vmsMetadata);
    }

    public static IVmsEventHandler build(final VmsMetadata vmsMetadata){
        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .build();
        return buildActualWebSocketClient(httpClient, vmsMetadata);
    }

    private static IVmsEventHandler buildActualWebSocketClient( HttpClient httpClient, final VmsMetadata vmsMetadata ){

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( vmsMetadata.queueToEventMap() );

        VmsWebSocketClient webSocketClient = new VmsWebSocketClient(serdes, vmsMetadata.internalPubSubService());

        // Not waiting to see whether this connection has been established
        httpClient.newWebSocketBuilder()//.connectTimeout(Duration.ofSeconds(10))
                .buildAsync(URI.create("ws://127.0.0.1:80"), webSocketClient);

        return webSocketClient;
    }

}