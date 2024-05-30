package dk.ku.di.dms.vms.web_common;

import jdk.net.ExtendedSocketOptions;

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;

import static java.net.StandardSocketOptions.*;

public final class NetworkUtils {

    public static void configure(AsynchronousSocketChannel channel, int osBufferSize) throws IOException {
        channel.setOption(TCP_NODELAY, false);
        channel.setOption(SO_KEEPALIVE, true);

        if(channel.supportedOptions().contains(ExtendedSocketOptions.TCP_QUICKACK)) {
            channel.setOption(ExtendedSocketOptions.TCP_QUICKACK, false);
        }

        if(osBufferSize > 0) {
            channel.setOption(SO_SNDBUF, osBufferSize);
            channel.setOption(SO_RCVBUF, osBufferSize);
        }
    }

}
