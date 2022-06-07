package dk.ku.di.dms.vms.coordinator.metadata;

import dk.ku.di.dms.vms.web_common.meta.VmsDataSchema;
import dk.ku.di.dms.vms.web_common.meta.VmsEventSchema;

import java.util.List;

/**
 * The identification of a connecting DBMS daemon
 * ideally one vms per dbms proxy, but in the future may be many.
 * but who knows... the sdk are already sending a map...
 */
public class VmsIdentifier {

    public final String name; // vms name

    // the node
    public String host;
    public int port;

    public long lastOffset;

    // data model
    public List<VmsDataSchema> dataSchema;

    // event data model
    public List<VmsEventSchema> eventSchema;

    public VmsIdentifier(String name, String host, int port, long lastOffset, List<VmsDataSchema> dataSchema, List<VmsEventSchema> eventSchema) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.lastOffset = lastOffset;
        this.dataSchema = dataSchema;
        this.eventSchema = eventSchema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VmsIdentifier that = (VmsIdentifier) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

}
