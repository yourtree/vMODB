package dk.ku.di.dms.vms.modb.common.schema.network;

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
 *
 * I'm relying on the fact that VMSs do not switch name, host, and port
 */
public class VmsIdentifier extends NetworkNode {

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

    /** Attributes below do not form an identification of the VMS, but rather
     * make it easier to manage metadata about each
     */

    public final Map<Long, List<TransactionEvent.Payload>> transactionEventsPerBatch;

    public final List<TransactionEvent.Payload> pendingWrites;

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
        this.pendingWrites = new ArrayList<>(10);
        this.transactionEventsPerBatch = new HashMap<>();
    }

    public String getIdentifier(){
        return vmsIdentifier;
    }

    public long getLastTid(){
        return lastTid;
    }

    public NetworkNode asNetworkNode(){
        return new NetworkNode(this.host, this.port);
    }

}
