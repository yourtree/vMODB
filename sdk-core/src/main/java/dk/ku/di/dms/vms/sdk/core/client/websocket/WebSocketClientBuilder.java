package dk.ku.di.dms.vms.sdk.core.client.websocket;

import dk.ku.di.dms.vms.sdk.core.event.handler.IVmsEventHandler;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletableFuture;

public class WebSocketClientBuilder {

    public static CompletableFuture<WebSocket> build(final IVmsSerdesProxy serdes, final IVmsEventHandler eventHandler){

        WebSocketClient webSocketClient = new WebSocketClient(serdes, eventHandler);

        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                //.executor(executor)
                .build();

        return client.newWebSocketBuilder()//.connectTimeout(Duration.ofSeconds(10))
                .buildAsync(URI.create("ws://127.0.0.1:80"), webSocketClient);

    }

}