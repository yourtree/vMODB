package dk.ku.di.dms.vms.modb.common.schema.network.meta;

import java.util.Objects;

/**
 * In distributed systems, nodes can fail.
 * When they come back, their network address may change,
 * but not their logical representation.
 * A transition: off -> on -> off
 * Immutable object for host and port.
 */
public class NetworkNode {

    public String host;
    public int port;

    private final transient int hashCode;

    // whether this node is active
    private volatile transient boolean active;

    public NetworkNode(String host, int port) {
        this.host = host;
        this.port = port;
        this.hashCode = Objects.hash(this.host, this.port);
        this.active = false;
    }

    // mutable since the VMS can crash
    @Override
    public int hashCode() {
        return this.hashCode;
    }

    @Override
    public boolean equals(Object o) {
        return hashCode() == o.hashCode();
    }

    public boolean isActive(){
        return this.active;
    }

    public void on(){
        this.active = true;
    }

    public void off(){
        this.active = false;
    }

    /*
     * Useful in case a VMS crashes, comes back online, but the metadata is kept
     * No need to resending
     */
//    public void update(String host, int port){
//        this.host = host;
//        this.port = port;
//        this.hashCode = Objects.hash(this.host, this.port);
//    }

}
