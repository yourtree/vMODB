package dk.ku.di.dms.vms.sdk.core.client.websocket;

import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;
import dk.ku.di.dms.vms.sdk.core.event.IVmsEventHandler;

import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;
import java.util.logging.Logger;

import static java.util.logging.Logger.GLOBAL_LOGGER_NAME;
import static java.util.logging.Logger.getLogger;

/**
 * TODO: look into
 *          https://crunchify.com/java-nio-non-blocking-io-with-server-client-example-java-nio-bytebuffer-and-channels-selector-java-nio-vs-io/
 */
public class WebSocketClient implements WebSocket.Listener {

    private static final Logger logger = getLogger(GLOBAL_LOGGER_NAME);

    private final IVmsSerdesProxy serdes;

    private final IVmsEventHandler eventHandler;

    public WebSocketClient(final IVmsSerdesProxy serdes, IVmsEventHandler eventHandler) {
        this.serdes = serdes;
        this.eventHandler = eventHandler;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        System.out.println("ERRRROR");
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        logger.info("onOpen using subprotocol " + webSocket.getSubprotocol());

        // send vms schema

    }

    /**
     * The expected payload is { tid : val, queue : val, event : object-val }
     *
     * @param webSocket
     * @param message
     * @param last
     * @return
     */
    public CompletionStage<Void> onText(WebSocket webSocket,
                                     CharSequence message,
                                     boolean last) {

        logger.info("onText using subprotocol " + message);
        TransactionalEvent event = serdes.fromJson(message.toString());

        eventHandler.accept(event);

        return null;
    }

}
