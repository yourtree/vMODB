package dk.ku.di.dms.vms.modb.common.schema.network.node;

public class IdentifiableNode extends NetworkNode {

    // identifier is the vms name
    public final String identifier;

    public IdentifiableNode(String identifier, String host, int port) {
        super(host, port);
        this.identifier = identifier;
    }

    @Override
    public String toString() {
        return "{" +
                "identifier='"+ identifier + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof IdentifiableNode that)) return false;
        return this.identifier.contentEquals(that.identifier) && super.equals(that);
    }

}
