package dk.ku.di.dms.vms.coordinator.metadata;

import dk.ku.di.dms.vms.web_common.meta.VmsDataSchema;
import dk.ku.di.dms.vms.web_common.meta.VmsEventSchema;

import java.util.List;
import java.util.Objects;

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

    public volatile long lastTid;

    // batch offset, also monotonically increasing.
    // to avoid vms to process transactions from the
    // next batch while the current has not finished yet
    public volatile long lastBatch;

    // data model
    public List<VmsDataSchema> dataSchema;

    // event data model
    public List<VmsEventSchema> eventSchema;

    public VmsIdentifier(String name, String host, int port, long lastTid, long lastBatch, List<VmsDataSchema> dataSchema, List<VmsEventSchema> eventSchema) {
        this.name = Objects.requireNonNull(name);
        this.host = host;
        this.port = port;
        this.lastTid = lastTid;
        this.lastBatch = lastBatch;
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
