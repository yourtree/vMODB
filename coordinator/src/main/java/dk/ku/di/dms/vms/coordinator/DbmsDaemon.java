package dk.ku.di.dms.vms.coordinator;

import dk.ku.di.dms.vms.web_common.meta.VmsDataSchema;
import dk.ku.di.dms.vms.web_common.meta.VmsEventSchema;

import java.util.List;

/**
 * The identification of a connecting DBMS daemon
 */
public class DbmsDaemon {

    public String virtualMicroservice;

    // the node
    public String host;
    public int port;

    // ideally one vms per dbms proxy, but in the future may be many.. who knows... the sdk are already sending a map...

    // data model
    public List<VmsDataSchema> dataSchema;

    // event data model
    public List<VmsEventSchema> eventSchema;



}
