package dk.ku.di.dms.vms.web_common;

import jdk.net.ExtendedSocketOptions;

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;

import static java.net.StandardSocketOptions.*;

public final class NetworkUtils {

    public static void configure(AsynchronousSocketChannel channel, int networkBufferSize) throws IOException {

        channel.setOption(TCP_NODELAY, true);
        channel.setOption(SO_KEEPALIVE, true);

        if(channel.supportedOptions().contains(ExtendedSocketOptions.TCP_QUICKACK)) {
            channel.setOption(ExtendedSocketOptions.TCP_QUICKACK, true);
        }

        if(channel.supportedOptions().contains(ExtendedSocketOptions.IP_DONTFRAGMENT)) {
            channel.setOption(ExtendedSocketOptions.IP_DONTFRAGMENT, true);
        }

        if(networkBufferSize > 0) {
            channel.setOption(SO_SNDBUF, networkBufferSize);
            channel.setOption(SO_RCVBUF, networkBufferSize);
        }

    }

}
