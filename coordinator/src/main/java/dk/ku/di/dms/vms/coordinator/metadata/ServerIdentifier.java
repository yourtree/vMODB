package dk.ku.di.dms.vms.coordinator.metadata;

import java.util.Objects;

/**
 * Basic data of a server running for leader
 */
public final class ServerIdentifier extends NetworkObject {

    // maybe volatile?
    public long lastOffset;

    public ServerIdentifier(String host, int port) {
        super(host,port);
        this.lastOffset = 0L;
    }

    public ServerIdentifier(String host, int port, long lastOffset) {
        super(host,port);
        this.lastOffset = lastOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServerIdentifier that = (ServerIdentifier) o;
        return port == that.port && Objects.equals(host, that.host);
    }

}
