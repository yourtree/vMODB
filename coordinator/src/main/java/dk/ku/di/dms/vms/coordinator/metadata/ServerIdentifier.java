package dk.ku.di.dms.vms.coordinator.metadata;

import java.util.Objects;

/**
 * Basic data of a server running for leader
 */
public class ServerIdentifier {

    public final String host;
    public final int port;

    public long lastOffset;

    // faster than dealing with string operation. yes, skew of the machine, but it does not matter
    public long timestamp;

    private final int hashCode;

    public ServerIdentifier(String host, int port) {
        this.host = host;
        this.port = port;
        this.lastOffset = 0L;
        this.timestamp = 0L;
        this.hashCode = Objects.hash(host, port);
    }

    public ServerIdentifier(String host, int port, long lastOffset, long timestamp) {
        this.host = host;
        this.port = port;
        this.lastOffset = lastOffset;
        this.timestamp = timestamp;
        this.hashCode = Objects.hash(host, port);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServerIdentifier that = (ServerIdentifier) o;
        return port == that.port && Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

}
