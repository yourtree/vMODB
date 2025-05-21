package dk.ku.di.dms.vms.web_common;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.channels.AsynchronousSocketChannel;

import dk.ku.di.dms.vms.web_common.NetworkUtils.ExtendedSocketOptions;
import dk.ku.di.dms.vms.web_common.channel.IChannel;

public final class NetworkUtils {

    private static final System.Logger LOGGER = System.getLogger(NetworkUtils.class.getName());

    public static void configure(AsynchronousSocketChannel channel, int soBufferSize) throws IOException {
        channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        if (channel.supportedOptions().contains(ExtendedSocketOptions.TCP_QUICKACK)) {
            channel.setOption(ExtendedSocketOptions.TCP_QUICKACK, false);
        }
        if (soBufferSize > 0) {
            LOGGER.log(System.Logger.Level.DEBUG, "Configuring channel " + channel + " with " + soBufferSize + " as size of SO_SNDBUF and SO_RCVBUF");
            channel.setOption(StandardSocketOptions.SO_SNDBUF, soBufferSize);
            channel.setOption(StandardSocketOptions.SO_RCVBUF, soBufferSize);
        }
    }

    public static void configure(IChannel channel_, int soBufferSize) throws IOException {
        if (channel_ instanceof AsynchronousSocketChannel channel) {
            configure(channel, soBufferSize);
        } else {
            // Check if this is an IoUringChannel using reflection to avoid direct dependency
            configureChannelByReflection(channel_, soBufferSize);
        }
    }

    private static void configureChannelByReflection(Object channel, int soBufferSize) {
        try {
            if (channel.getClass().getSimpleName().equals("IoUringAsyncChannel")) {
                // If it's IoUringAsyncChannel, get the underlying IoUringChannel
                Method getChannelMethod = channel.getClass().getMethod("getChannel");
                Object ioUringChannel = getChannelMethod.invoke(channel);
                configureIoUringByReflection(ioUringChannel, soBufferSize, soBufferSize);
            } else if (channel.getClass().getSimpleName().equals("IoUringChannel")) {
                // If it's IoUringChannel directly, configure it
                configureIoUringByReflection(channel, soBufferSize, soBufferSize);
            }
        } catch (Exception e) {
            LOGGER.log(System.Logger.Level.ERROR, "Failed to configure channel using reflection: " + e.getMessage(), e);
        }
    }

    private static void configureIoUringByReflection(Object ioUringChannel, int soRcvBuf, int soSndBuf) {
        try {
            Method configureSocketMethod = ioUringChannel.getClass().getMethod("configureSocket", int.class, int.class);
            configureSocketMethod.invoke(ioUringChannel, soRcvBuf, soSndBuf);
        } catch (Exception e) {
            LOGGER.log(System.Logger.Level.ERROR, "Failed to configure IoUringChannel: " + e.getMessage(), e);
        }
    }

    public static void configureForFastAck(AsynchronousSocketChannel channel) {
        try {
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            if(channel.supportedOptions().contains(ExtendedSocketOptions.TCP_QUICKACK)) {
                channel.setOption(ExtendedSocketOptions.TCP_QUICKACK, true);
            }
        } catch (IOException e) {
            LOGGER.log(System.Logger.Level.ERROR, "Error: "+e.getMessage(), e);
        }
    }

    /**
     * Class for extended socket options, like TCP_QUICKACK which is not available 
     * in the standard socket options.
     */
    public static class ExtendedSocketOptions {
        public static final SocketOption<Boolean> TCP_QUICKACK = new SocketOption<Boolean>() {
            @Override
            public String name() {
                return "TCP_QUICKACK";
            }

            @Override
            public Class<Boolean> type() {
                return Boolean.class;
            }
        };
    }
}
