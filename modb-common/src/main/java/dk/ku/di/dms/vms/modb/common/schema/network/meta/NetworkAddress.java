package dk.ku.di.dms.vms.modb.common.schema.network.meta;

import dk.ku.di.dms.vms.modb.common.schema.network.node.NetworkNode;

import java.net.InetSocketAddress;
import java.util.Objects;

public class NetworkAddress {

    private final int hashCode;

    public final String host;
    public final int port;

    public NetworkAddress(String host, int port) {
        this.host = host;
        this.port = port;
        this.hashCode = Objects.hash(this.host, this.port);
    }

    @Override
    public String toString() {
        return "{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }

    public InetSocketAddress asInetSocketAddress(){
        return new InetSocketAddress(this.host, this.port);
    }

    // mutable since the VMS can crash
    @Override
    public int hashCode() {
        return this.hashCode;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof NetworkNode && hashCode() == o.hashCode();
    }

}
