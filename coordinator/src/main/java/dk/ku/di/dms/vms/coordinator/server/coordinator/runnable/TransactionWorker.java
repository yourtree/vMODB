package dk.ku.di.dms.vms.coordinator.server.coordinator.runnable;

import dk.ku.di.dms.vms.coordinator.server.coordinator.batch.BatchContext;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.EventIdentifier;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.*;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;

public class TransactionWorker extends StoppableRunnable {

    private static final VarHandle TID_COUNT;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            TID_COUNT = l.findVarHandle(TransactionWorker.class, "tid", long.class);
        } catch (Exception e) {
            throw new InternalError(e);
        }
    }

    private static final System.Logger LOGGER = System.getLogger(TransactionWorker.class.getName());

    private final int id;

    private final Deque<TransactionInput> deque;
    private long startingTidBatch;
    private long tid;
    private final int maxNumberOfTIDsBatch;
    private final int batchWindow;
    private final int numWorkers;

    private final Map<String, TransactionDAG> transactionMap;
    private final Map<String, VmsTracking[]> vmsPerTransactionMap;
    private final Map<String, VmsTracking> vmsTrackingMap;
    private final Map<String, IVmsWorker> vmsWorkerContainerMap;

    private final IVmsSerdesProxy serdesProxy;

    private BatchContext batchContext;
    private final TreeMap<Long, PendingTransactionInput> pendingInputMap;

    private final Queue<Map<String, PrecendenceInfo>> precedenceMapInputQueue;
    private final Queue<Map<String, PrecendenceInfo>> precedenceMapOutputQueue;
    private final Map<Long, Map<String, PrecendenceInfo>> precedenceMapCache;

    private static class VmsTracking {
        public final String identifier;
        public long batch;
        public long lastTid;
        public long previousBatch;
        public int numberOfTIDsCurrentBatch;

        public VmsTracking(VmsNode vmsNode) {
            this.identifier = vmsNode.identifier;
            this.batch = vmsNode.batch;
            this.lastTid = vmsNode.lastTid;
            this.previousBatch = vmsNode.previousBatch;
            this.numberOfTIDsCurrentBatch = vmsNode.numberOfTIDsCurrentBatch;
        }
    }

    private record PendingTransactionInput (long tid, TransactionInput input){}

    @SuppressWarnings("ToArrayCallWithZeroLengthArrayArgument")
    public static TransactionWorker build(int id, Deque<TransactionInput> deque, long startingTid, int maxNumberOfTIDsBatch, int batchWindow, int numWorkers, Queue<Map<String, PrecendenceInfo>> precedenceMapInputQueue, Queue<Map<String, PrecendenceInfo>> precedenceMapOutputQueue, Map<String, TransactionDAG> transactionMap, Map<String, VmsNode[]> vmsIdentifiersPerDAG, Map<String, IVmsWorker> vmsWorkerContainerMap, IVmsSerdesProxy serdesProxy){

        // build private VmsTracking objects
        Map<String, VmsTracking> vmsTrackingMap = new HashMap<>();
        Map<String, VmsTracking[]> vmsPerTransactionMap = new HashMap<>(vmsIdentifiersPerDAG.size());
        for(var txEntry : vmsIdentifiersPerDAG.entrySet()){
            var list = new ArrayList<VmsTracking>();
            for(VmsNode vmsNode : txEntry.getValue()){
                var vms = vmsTrackingMap.putIfAbsent(vmsNode.identifier, new VmsTracking(vmsNode));
                if(vms != null){
                    list.add(vms);
                }
            }
            vmsPerTransactionMap.put(txEntry.getKey(), list.toArray(new VmsTracking[list.size()]));
        }
        return new TransactionWorker(id, deque, startingTid, maxNumberOfTIDsBatch, batchWindow, numWorkers, precedenceMapInputQueue, precedenceMapOutputQueue, transactionMap, vmsPerTransactionMap, vmsTrackingMap, vmsWorkerContainerMap, serdesProxy);
    }

    private TransactionWorker(int id, Deque<TransactionInput> deque, long startingTidBatch, int maxNumberOfTIDsBatch, int batchWindow, int numWorkers, Queue<Map<String, PrecendenceInfo>> precedenceMapInputQueue, Queue<Map<String, PrecendenceInfo>> precedenceMapOutputQueue, Map<String, TransactionDAG> transactionMap, Map<String, VmsTracking[]> vmsPerTransactionMap, Map<String, VmsTracking> vmsTrackingMap, Map<String, IVmsWorker> vmsWorkerContainerMap, IVmsSerdesProxy serdesProxy){
        this.id = id;
        this.deque = deque;
        this.startingTidBatch = startingTidBatch;
        this.tid = startingTidBatch;
        this.maxNumberOfTIDsBatch = maxNumberOfTIDsBatch;
        this.batchWindow = batchWindow;
        this.numWorkers = numWorkers;
        this.precedenceMapInputQueue = precedenceMapInputQueue;
        this.precedenceMapOutputQueue = precedenceMapOutputQueue;
        this.transactionMap = transactionMap;
        this.vmsPerTransactionMap = vmsPerTransactionMap;
        this.vmsTrackingMap = vmsTrackingMap;
        this.vmsWorkerContainerMap = vmsWorkerContainerMap;
        this.serdesProxy = serdesProxy;

        this.pendingInputMap = new TreeMap<>();
        this.precedenceMapCache = new HashMap<>();

        // define first batch context based on data from constructor
        long startingBatchOffset = (this.startingTidBatch + maxNumberOfTIDsBatch - 1) / maxNumberOfTIDsBatch;
        this.batchContext = new BatchContext(startingBatchOffset);
    }

    @Override
    public void run() {
        LOGGER.log(INFO, "Starting transaction worker #" + id);
        TransactionInput data;
        long lastTidBatch;
        long end;
        while (this.isRunning()) {
            lastTidBatch = this.tid + this.maxNumberOfTIDsBatch - 1;
            end = System.currentTimeMillis() + this.batchWindow;
            do {
                // drain transaction inputs
                while ((data = this.deque.poll()) != null &&
                        // avoid calling currentTime for every item
                        this.tid <= lastTidBatch) {
                    // process precedence from previous worker in the ring
                    // we could do it in advance current batch, but can lead to higher wait in vms
                    this.queueTransactionInput(data);
                    this.processPendingInput();
                }
            } while (this.tid <= lastTidBatch || System.currentTimeMillis() < end);
            while(!this.advanceCurrentBatch() && this.isRunning()){
                this.processPendingInput();
            }
            this.tid = (lastTidBatch * numWorkers) + 1;
            this.startingTidBatch = this.tid;
        }
    }

    private void queueTransactionInput(TransactionInput transactionInput) {
        TransactionDAG transactionDAG = this.transactionMap.get( transactionInput.name );
        EventIdentifier event = transactionDAG.inputEvents.get(transactionInput.event.name);
        // get the vms
        VmsTracking inputVms = this.vmsTrackingMap.get(event.targetVms);
        VmsTracking[] vmsList = this.vmsPerTransactionMap.get( transactionDAG.name );

        // this hashmap can be reused
        Map<String, Long> previousTidPerVms = new HashMap<>(vmsList.length);

        // if any vms in the dag shows a previous batch offset, then this input must be marked as pending
        // until we get the precedence from the transaction worker that precedes this one in the ring,
        // we cannot submit this input
        for (var vms_ : vmsList) {
            previousTidPerVms.put(vms_.identifier, vms_.lastTid);
            if(vms_.batch != this.batchContext.batchOffset){
                // previous batch will be updated later, when precedence map is received from another worker
                // vms_.previousBatch = vms_.batch;
                vms_.batch = this.batchContext.batchOffset;
            }
            vms_.lastTid = this.tid;
            vms_.numberOfTIDsCurrentBatch++;
            break;
        }

        this.batchContext.terminalVMSs.addAll( transactionDAG.terminalNodes );

        if(this.tid == startingTidBatch){
            // this has to be emmitted when batch info from previous worker in the ring arrives
            this.pendingInputMap.put(this.batchContext.batchOffset - 1,
                    new PendingTransactionInput(this.tid, transactionInput));
        } else {
            String precedenceMapStr = this.serdesProxy.serializeMap(previousTidPerVms);
            TransactionEvent.PayloadRaw txEvent = TransactionEvent.of(this.tid, this.batchContext.batchOffset,
                    transactionInput.event.name, transactionInput.event.payload, precedenceMapStr);
               /*
                *   if(!event.targetVms.equalsIgnoreCase(inputVms.identifier)){
                        LOGGER.log(ERROR,"Leader: The event was going to be queued to the incorrect VMS worker!");
                    }
                */
            LOGGER.log(DEBUG,"Leader: Transaction worker "+id+" adding event "+event.name+" to "+inputVms.identifier+" worker:\n"+txEvent);
            this.vmsWorkerContainerMap.get(inputVms.identifier).queueTransactionEvent(txEvent);
        }
        this.tid++;
    }

    private void processPendingInput() {
        // comes in order always
        Map<String, PrecendenceInfo> precedenceMap = this.precedenceMapInputQueue.poll();
        if(precedenceMap == null){ return; }

        var entry = this.pendingInputMap.firstEntry();
        long batchOffset = entry.getKey();

        // building map only those VMSs that participate in the transaction
        TransactionDAG transactionDAG = this.transactionMap.get( entry.getValue().input.name );
        VmsTracking[] vmsList = this.vmsPerTransactionMap.get( transactionDAG.name );
        Map<String, Long> previousTidPerVms = new HashMap<>(vmsList.length);

        for (var vms_ : vmsList) {
            var precedenceInfo = precedenceMap.get(vms_.identifier);
            previousTidPerVms.put(vms_.identifier, precedenceInfo.lastTid());
            break;
        }
        String precedenceMapStr = this.serdesProxy.serializeMap(previousTidPerVms);

        EventIdentifier event = transactionDAG.inputEvents.get( entry.getValue().input.event.name);
        VmsTracking inputVms = this.vmsTrackingMap.get(event.targetVms);
        TransactionEvent.PayloadRaw txEvent = TransactionEvent.of(entry.getValue().tid, batchOffset,
                entry.getValue().input.event.name, entry.getValue().input.event.payload, precedenceMapStr);
        this.vmsWorkerContainerMap.get(inputVms.identifier).queueTransactionEvent(txEvent);

        this.pendingInputMap.remove(entry.getKey());

        // store precedenceMap for processing inside advanceCurrentBatch
        // store for batch completion time
        this.precedenceMapCache.put(batchOffset, precedenceMap);
    }

    public record PrecendenceInfo(long lastTid, long lastBatch, long previousToLastBatch){}

    private boolean advanceCurrentBatch() {

        if(!this.precedenceMapCache.containsKey(this.batchContext.batchOffset - 1)) {
            return false;
        }

        var precedenceMap = this.precedenceMapCache.get(this.batchContext.batchOffset - 1);

        // cannot issue a batch if we don't know the last batch of each VMS in this batch
        // the last batch might have been updated by previous workers in the ring
        // update for those vms that have not participated in this batch
        for(var vmsEntry : precedenceMap.entrySet()){
            VmsTracking inputVms = this.vmsTrackingMap.get(vmsEntry.getKey());
            if(inputVms.batch != this.batchContext.batchOffset){
                inputVms.previousBatch = vmsEntry.getValue().previousToLastBatch;
                inputVms.batch = vmsEntry.getValue().lastBatch;
                inputVms.lastTid = vmsEntry.getValue().lastTid;
            } else {
                // should not update last tid here.
                // only need the last tid coming from a worker for the transaction input precedence map
                inputVms.previousBatch = vmsEntry.getValue().lastBatch;
            }
        }

        // iterate over all vms in the last batch, filter those which last tid != this.tid
        // after filtering, send a map containing the vms (identifier) and their corresponding last tids to the next transaction worker in the ring
        // must save this in a batch context map?
        this.batchContext = new BatchContext(this.batchContext.batchOffset + 1);
        // build batch precedence map for next worker in the ring
        Map<String, PrecendenceInfo> precedenceInfoPerVms = new HashMap<>(this.vmsTrackingMap.size());
        for (var vms_ : this.vmsTrackingMap.values()) {
            precedenceInfoPerVms.put(vms_.identifier, new PrecendenceInfo(vms_.lastTid, vms_.batch, vms_.previousBatch));
        }
        this.precedenceMapOutputQueue.add(precedenceInfoPerVms);

        return true;
    }

    public long getTid(){
        return (long) TID_COUNT.getVolatile(this);
    }

}
