package dk.ku.di.dms.vms.sdk.core.client.websocket;

import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;
import dk.ku.di.dms.vms.sdk.core.event.handler.IVmsEventHandler;
import dk.ku.di.dms.vms.sdk.core.event.pubsub.IVmsInternalPubSubService;

import java.net.http.WebSocket;
import java.util.concurrent.CompletionStage;
import java.util.logging.Logger;

import static java.util.logging.Logger.GLOBAL_LOGGER_NAME;
import static java.util.logging.Logger.getLogger;

/**
 * TODO: look into
 *          https://crunchify.com/java-nio-non-blocking-io-with-server-client-example-java-nio-bytebuffer-and-channels-selector-java-nio-vs-io/
 */
class VmsWebSocketClient implements WebSocket.Listener, IVmsEventHandler {

    private static final Logger logger = getLogger(GLOBAL_LOGGER_NAME);

    private final IVmsSerdesProxy serdes;

    private final IVmsInternalPubSubService internalPubSubService;

    private WebSocket webSocket;

    public VmsWebSocketClient(final IVmsSerdesProxy serdes, final IVmsInternalPubSubService internalPubSubService) {
        this.serdes = serdes;
        this.internalPubSubService = internalPubSubService;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        System.out.println("ERRRROR");

        // what should we do here?
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        logger.info("onOpen " + webSocket.getSubprotocol());
        this.webSocket = webSocket;
        // TODO send vms schema

    }

    /**
     * The expected payload is { tid : val, queue : val, event : object-val }
     */
    @Override
    public CompletionStage<WebSocket> onText(WebSocket webSocket,
                                     CharSequence message,
                                     boolean last) {

        logger.info("onText " + message);
        TransactionalEvent event = serdes.fromJson(message.toString());

        this.handle(event);

        return null;
    }

    @Override
    public void handle(TransactionalEvent event) {
        // this should not be on the critical path on ingesting events
        internalPubSubService.inputQueue().add( event );
    }

    @Override
    public void expel(TransactionalEvent event) {

        // TODO check using Flow. Subscriber
        // https://stackoverflow.com/questions/50112809/publishing-data-on-java-9-flow-to-subscribers-in-a-way-that-only-one-subscriber

        String json = this.serdes.toJson( event );
        this.webSocket.sendText( json, true );
    }

}
