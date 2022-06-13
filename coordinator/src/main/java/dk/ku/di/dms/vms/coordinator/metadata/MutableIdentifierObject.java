package dk.ku.di.dms.vms.coordinator.metadata;

import java.util.Objects;

/**
 * In distributed systems, nodes can fail. When they come back, their network address may change, but not their logical representation.
 */
public class MutableIdentifierObject {

    public String host;
    public int port;

    private int hashCode;

    // mutable since the VMS can crash
    @Override
    public int hashCode() {
        return hashCode;
    }

    public MutableIdentifierObject(String host, int port) {
        this.host = host;
        this.port = port;
        this.hashCode = Objects.hash(this.host, this.port);
    }

    // must be data race free
    public void setNewAddress(String host, int port){
        this.host = host;
        this.port = port;
        this.hashCode = Objects.hash(this.host, this.port);
    }

}
