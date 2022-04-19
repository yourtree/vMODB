package dk.ku.di.dms.vms.sdk.core.client.websocket;

import dk.ku.di.dms.vms.modb.common.event.IEvent;
import dk.ku.di.dms.vms.modb.common.meta.VmsSchema;
import dk.ku.di.dms.vms.sdk.core.event.handler.IVmsEventHandler;
import dk.ku.di.dms.vms.sdk.core.event.pubsub.IVmsInternalPubSubService;
import dk.ku.di.dms.vms.web_common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.web_common.serdes.VmsSerdesProxyBuilder;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

public class WebSocketHandlerBuilder {

    public static IVmsEventHandler build(final ExecutorService executor,
                                         final IVmsInternalPubSubService internalPubSubService,
                                         final Function<String,Class<? extends IEvent>> queueToEventTypeResolver,
                                         final Map<String, VmsSchema> vmsSchema){
        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .executor(executor)
                .build();
        return buildActualWebSocketClient(httpClient, internalPubSubService, queueToEventTypeResolver, vmsSchema);
    }

    public static IVmsEventHandler build(final IVmsInternalPubSubService internalPubSubService,
                                         final Function<String,Class<? extends IEvent>> queueToEventTypeResolver,
                                         final Map<String, VmsSchema> vmsSchema){
        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .build();
        return buildActualWebSocketClient(httpClient, internalPubSubService, queueToEventTypeResolver, vmsSchema);
    }

    private static IVmsEventHandler buildActualWebSocketClient( final HttpClient httpClient,
                                                                final IVmsInternalPubSubService internalPubSubService,
                                                                final Function<String,Class<? extends IEvent>> queueToEventTypeResolver,
                                                                final Map<String, VmsSchema> vmsSchema){

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build( queueToEventTypeResolver );

        VmsWebSocketClient webSocketClient = new VmsWebSocketClient(serdes, internalPubSubService, vmsSchema);

        // Not waiting to see whether this connection has been established
        httpClient.newWebSocketBuilder()//.connectTimeout(Duration.ofSeconds(10))
                .buildAsync(URI.create("ws://127.0.0.1:80"), webSocketClient);


        //

        return null;
    }

}