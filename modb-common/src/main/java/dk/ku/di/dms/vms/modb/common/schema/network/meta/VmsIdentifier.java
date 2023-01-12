package dk.ku.di.dms.vms.modb.common.schema.network.meta;

import dk.ku.di.dms.vms.modb.common.schema.VmsDataSchema;
import dk.ku.di.dms.vms.modb.common.schema.VmsEventSchema;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The identification of a connecting DBMS daemon
 * ideally one vms per dbms proxy, but in the future may be many.
 * but who knows... the sdk are already sending a map...
 * I'm relying on the fact that VMSs do not switch name, host, and port
 * This class is supposed to be used by the coordinator to find about
 * producers and consumers and then form and send the consumer set
 * to each virtual microservice
 */
public class VmsIdentifier extends ConsumerVms {

    // identifier is the vms name
    public final String vmsIdentifier;

    public long lastTid;

    // batch offset, also monotonically increasing.
    // to avoid vms to process transactions from the
    // next batch while the current has not finished yet
    public long lastBatch;

    // data model
     public final Map<String, VmsDataSchema> dataSchema;

    // event data model
    public final Map<String, VmsEventSchema> inputEventSchema;

    public final Map<String, VmsEventSchema> outputEventSchema;

    public VmsIdentifier(String host, int port, String vmsIdentifier, long lastTid, long lastBatch,
                         Map<String, VmsDataSchema> dataSchema,
                         Map<String, VmsEventSchema> inputEventSchema,
                         Map<String, VmsEventSchema> outputEventSchema) {
        super(host, port);
        this.vmsIdentifier = vmsIdentifier;
        this.lastTid = lastTid;
        this.lastBatch = lastBatch;
        this.dataSchema = dataSchema;
        this.inputEventSchema = inputEventSchema;
        this.outputEventSchema = outputEventSchema;
    }

    public long getLastTid(){
        return this.lastTid;
    }

    public String getIdentifier(){
        return this.vmsIdentifier;
    }

}
