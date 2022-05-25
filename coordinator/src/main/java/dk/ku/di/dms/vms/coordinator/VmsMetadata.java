package dk.ku.di.dms.vms.coordinator;

import dk.ku.di.dms.vms.web_common.meta.VmsDataSchema;
import dk.ku.di.dms.vms.web_common.meta.VmsEventSchema;

import java.util.List;

/**
 * The identification of a connecting DBMS daemon
 * ideally one vms per dbms proxy, but in the future may be many..
 * but who knows... the sdk are already sending a map...
 */
public class VmsMetadata {

    public String name; // vms name

    // the node
    public String host;
    public int port;

    public long lastOffset;

    // data model
    public List<VmsDataSchema> dataSchema;

    // event data model
    public List<VmsEventSchema> eventSchema;



}
