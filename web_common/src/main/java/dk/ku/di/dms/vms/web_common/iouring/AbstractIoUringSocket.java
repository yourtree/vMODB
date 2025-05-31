package dk.ku.di.dms.vms.web_common.iouring;

import dk.ku.di.dms.vms.web_common.iouring.util.NativeLibraryLoader;
import java.io.IOException;

/**
 * An {@link AbstractIoUringChannel} representing a network socket.
 */
public class AbstractIoUringSocket extends AbstractIoUringChannel {
    private final String ipAddress;
    private final int port;

    /**
     * Creates a new {@code AbstractIoUringSocket} instance.
     * @param fd The file descriptor
     * @param ipAddress The IP address
     * @param port The port
     */
    AbstractIoUringSocket(int fd, String ipAddress, int port) {
        super(fd);
        this.ipAddress = ipAddress;
        this.port = port;
    }

    public String ipAddress() {
        return ipAddress;
    }

    public int port() {
        return port;
    }

    static native int create();

    /**
     * Sets a socket option on the underlying socket.
     * @param optionId The socket option identifier (1=SO_SNDBUF, 2=SO_RCVBUF, 3=SO_KEEPALIVE, 4=SO_REUSEADDR, 5=TCP_NODELAY)
     * @param value The option value (integer for buffer sizes, 0/1 for boolean options)
     */
    native void setSocketOption(int optionId, int value) throws IOException;

    /**
     * Gets a socket option from the underlying socket.
     * @param optionId The socket option identifier (1=SO_SNDBUF, 2=SO_RCVBUF, 3=SO_KEEPALIVE, 4=SO_REUSEADDR, 5=TCP_NODELAY)
     * @return The option value (integer for buffer sizes, 0/1 for boolean options)
     */
    native int getSocketOption(int optionId) throws IOException;

    static {
        NativeLibraryLoader.load();
    }
}
