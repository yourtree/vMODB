package dk.ku.di.dms.vms.modb.common.schema.network.meta;

import java.net.InetSocketAddress;

public class NetworkAddress {

    public final String host;
    public final int port;

    public NetworkAddress(String host, int port) {
        this.host = host;
        this.port = port;
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

}
