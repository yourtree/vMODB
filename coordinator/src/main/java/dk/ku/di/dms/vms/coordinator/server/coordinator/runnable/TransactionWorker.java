package dk.ku.di.dms.vms.coordinator.server.coordinator.runnable;

import dk.ku.di.dms.vms.coordinator.server.coordinator.batch.BatchContext;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.EventIdentifier;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;

public class TransactionWorker extends StoppableRunnable {

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
    private final TreeMap<Long, List<PendingTransactionInput>> pendingInputMap;

    private final Queue<Map<String, PrecendenceInfo>> precedenceMapInputQueue;
    private final Queue<Map<String, PrecendenceInfo>> precedenceMapOutputQueue;
    private final Map<Long, Map<String, PrecendenceInfo>> precedenceMapCache;
    private final Queue<Long> lastTidQueue;

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
    public static TransactionWorker build(int id, Deque<TransactionInput> deque,
                                          long startingTid, int maxNumberOfTIDsBatch,
                                          int batchWindow, int numWorkers,
                                          Queue<Map<String, PrecendenceInfo>> precedenceMapInputQueue,
                                          Queue<Map<String, PrecendenceInfo>> precedenceMapOutputQueue,
                                          Map<String, TransactionDAG> transactionMap,
                                          Map<String, VmsNode[]> vmsIdentifiersPerDAG,
                                          Map<String, IVmsWorker> vmsWorkerContainerMap,
                                          IVmsSerdesProxy serdesProxy){
        // build private VmsTracking objects
        Map<String, VmsTracking> vmsTrackingMap = new HashMap<>();
        Map<String, VmsTracking[]> vmsPerTransactionMap = new HashMap<>(vmsIdentifiersPerDAG.size());
        for(var txEntry : vmsIdentifiersPerDAG.entrySet()){
            var list = new ArrayList<VmsTracking>();
            for(VmsNode vmsNode : txEntry.getValue()){
                if(!vmsTrackingMap.containsKey(vmsNode.identifier)){
                    vmsTrackingMap.putIfAbsent(vmsNode.identifier, new VmsTracking(vmsNode));
                }
                list.add(vmsTrackingMap.get(vmsNode.identifier));
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

        this.lastTidQueue = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void run() {
        LOGGER.log(INFO, "Starting transaction worker #" + this.id);
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
                }
            } while (this.tid <= lastTidBatch && System.currentTimeMillis() < end);
            do {
                this.processPendingInput();
            } while(!this.advanceCurrentBatch() && this.isRunning());

            // issue a batch complete signal
            this.signalLastTidCompleted(this.tid - 1);

            this.tid = this.getTidNextBatch();
            LOGGER.log(DEBUG, "Worker #" + id+" assigning TID "+this.tid);
            this.startingTidBatch = this.tid;
        }
    }

    private void signalLastTidCompleted(long tid) {
        this.lastTidQueue.add(tid);
    }

    public long getLastTidCompleted(){
        Long tid = this.lastTidQueue.poll();
        if(tid == null) return 0;
        return tid;
    }

    private long getTidNextBatch() {
        return this.startingTidBatch + ((long) this.numWorkers * this.maxNumberOfTIDsBatch);
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
        boolean markAsPending = false;
        for (var vms_ : vmsList) {
            previousTidPerVms.put(vms_.identifier, vms_.lastTid);
            if(vms_.batch != this.batchContext.batchOffset){
                // previous batch will be updated later, when precedence map is received from another worker
                // vms_.previousBatch = vms_.batch;
                vms_.batch = this.batchContext.batchOffset;
                markAsPending = true;
            }
            vms_.lastTid = this.tid;
            vms_.numberOfTIDsCurrentBatch++;
            // ONLY SET BREAK IF TESTING A DAG WITH A SINGLE VMS
            // break;
        }

        this.batchContext.terminalVMSs.addAll( transactionDAG.terminalNodes );

        if(markAsPending){
            // this has to be emmitted when batch info from previous worker in the ring arrives
            long lastBatchOffset = this.batchContext.batchOffset - 1;
            List<PendingTransactionInput> list;
            if(this.pendingInputMap.containsKey(lastBatchOffset)){
                list = this.pendingInputMap.get(lastBatchOffset);
            } else {
                list = new ArrayList<>();
                this.pendingInputMap.put(lastBatchOffset, list);
            }
            list.add(new PendingTransactionInput(this.tid, transactionInput));
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

    public static class PrecendenceInfo {
        long lastTid;
        final long lastBatch;
        final long previousToLastBatch;
        public PrecendenceInfo(long lastTid, long lastBatch, long previousToLastBatch) {
            this.lastBatch = lastBatch;
            this.lastTid = lastTid;
            this.previousToLastBatch = previousToLastBatch;
        }

        public long lastTid() {
            return lastTid;
        }

        public long lastBatch() {
            return lastBatch;
        }

        public long previousToLastBatch() {
            return previousToLastBatch;
        }
    }

    private void processPendingInput() {
        // comes in order always
        Map<String, PrecendenceInfo> precedenceMap = this.precedenceMapInputQueue.poll();
        if(precedenceMap == null){ return; }

        var entry = this.pendingInputMap.firstEntry();
        long batchOffset = entry.getKey();
        List<PendingTransactionInput> list = entry.getValue();

        for(PendingTransactionInput pendingInput : list) {
            // building map only those VMSs that participate in the transaction
            TransactionDAG transactionDAG = this.transactionMap.get(pendingInput.input.name);
            VmsTracking[] vmsList = this.vmsPerTransactionMap.get(transactionDAG.name);
            Map<String, Long> previousTidPerVms = new HashMap<>(vmsList.length);

            for (VmsTracking vms_ : vmsList) {
                PrecendenceInfo precedenceInfo = precedenceMap.get(vms_.identifier);
                previousTidPerVms.put(vms_.identifier, precedenceInfo.lastTid);
                // update precedence info for next pending input
                precedenceInfo.lastTid = pendingInput.tid;
                // ONLY SET BREAK IF DAG CONTAINS A SINGLE VMS
                // break;
            }
            String precedenceMapStr = this.serdesProxy.serializeMap(previousTidPerVms);

            EventIdentifier event = transactionDAG.inputEvents.get(pendingInput.input.event.name);
            VmsTracking inputVms = this.vmsTrackingMap.get(event.targetVms);
            TransactionEvent.PayloadRaw txEvent = TransactionEvent.of(pendingInput.tid, batchOffset,
                    pendingInput.input.event.name, pendingInput.input.event.payload, precedenceMapStr);
            this.vmsWorkerContainerMap.get(inputVms.identifier).queueTransactionEvent(txEvent);
        }

        this.pendingInputMap.remove(entry.getKey());

        // store precedenceMap for processing inside advanceCurrentBatch
        // store for batch completion time
        this.precedenceMapCache.put(batchOffset, precedenceMap);
    }

    private boolean advanceCurrentBatch() {

        if(!this.precedenceMapCache.containsKey(this.batchContext.batchOffset - 1)) {
            return false;
        }

        var precedenceMap = this.precedenceMapCache.get(this.batchContext.batchOffset - 1);

        // cannot issue a batch if we don't know the last batch of each VMS in this batch
        // the last batch might have been updated by previous workers in the ring
        // update for those vms that have not participated in this batch
        for(var vmsEntry : precedenceMap.entrySet()){
            PrecendenceInfo precedenceInfo = vmsEntry.getValue();
            VmsTracking inputVms = this.vmsTrackingMap.get(vmsEntry.getKey());
            if(inputVms.batch != this.batchContext.batchOffset){
                inputVms.previousBatch = precedenceInfo.previousToLastBatch;
                inputVms.batch = precedenceInfo.lastBatch;
                inputVms.lastTid = precedenceInfo.lastTid;
            } else {
                // should not update last tid here.
                // only need the last tid coming from a worker for the transaction input precedence map
                inputVms.previousBatch = precedenceInfo.lastBatch;
            }
        }

        // iterate over all vms in the last batch, filter those which last tid != this.tid
        // after filtering, send a map containing the vms (identifier) and their corresponding last tids to the next transaction worker in the ring
        // must save this in a batch context map?
        this.batchContext = new BatchContext(this.batchContext.batchOffset + 1);
        // build batch precedence map for next worker in the ring
        Map<String, PrecendenceInfo> precedenceInfoPerVms = new HashMap<>(this.vmsTrackingMap.size());
        for (var vms_ : this.vmsTrackingMap.values()) {
            precedenceInfoPerVms.put(vms_.identifier, new PrecendenceInfo(
                    vms_.lastTid, vms_.batch, vms_.previousBatch));
        }
        this.precedenceMapOutputQueue.add(precedenceInfoPerVms);

        return true;
    }

}
