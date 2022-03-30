package dk.ku.di.dms.vms.sidecar;

import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;

public class VMSClient implements WebSocket.Listener {

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        System.out.println("ERRRROR");
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        System.out.println("onOpen using subprotocol " + webSocket.getSubprotocol());
        WebSocket.Listener.super.onOpen(webSocket);

        // webSocket.sendText("dededede", false);
        webSocket.request(1);
    }

    public CompletionStage<?> onBinary(WebSocket webSocket,
                                       ByteBuffer data,
                                       boolean last) {
        System.out.println("onOpen using onbinary " + webSocket.getSubprotocol());
        webSocket.request(1);

        return null;
    }

    StringBuilder text = new StringBuilder();

    public CompletionStage<?> onText(WebSocket webSocket,
                                     CharSequence message,
                                     boolean last) {
        System.out.println("onText using subprotocol " + message);
        text.append(message);
        if (last) {
            text = new StringBuilder();
        }
        webSocket.request(1);
        return null;
    }

}
