package dk.ku.di.dms.vms.coordinator.server.coordinator.runnable;

import dk.ku.di.dms.vms.coordinator.server.coordinator.batch.BatchContext;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.EventIdentifier;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.util.*;

import static java.lang.System.Logger.Level.DEBUG;

class TransactionWorker extends StoppableRunnable {

    // own copy of vms list
    // own tid
    // own batch id

    // starting batch = starting tid / batch max tx

    // next starting tid = (last tid of last batch * num tx issuer) + 1
    // next batch id = (next starting tid / num tx issuer)

    // last tid of batch = 1000
    //batch max size = 1000
    //
    //1000/1000 = 1 current batch
    //
    //1 + 4 = 5
    //4 is number of threads

    // no coordination is necessary, as long as the threads remain fixed

    private final Deque<TransactionInput> deque;
    private long tid;
    private long startingTid;
    private final int maxNumberOfTIDsBatch;
    private int batchWindow;
    private final int numWorkers;
    private final Map<String, TransactionDAG> transactionMap;
    private final Map<String, VmsTracking[]> vmsPerTransactionMap;
    private final Map<String, VmsTracking> vmsTrackingMap;
    private final Map<String, Coordinator.VmsWorkerContainer> vmsWorkerContainerMap;

    private final IVmsSerdesProxy serdesProxy;

    private BatchContext batchContext;
    private Map<Long, List<PendingTransactionInput>> pendingInputMap;

    // unit of data sent to another transaction worker
    private static class VmsPreviousBatchInfo {
        public String identifier;
        public long lastBatch;
        public long lastTid;
    }

    private static class PreviousBatchInfo {
        public long batchOffset;
        public Map<String, VmsPreviousBatchInfo> vmsPreviousBatchInfoMap;
    }

    private Queue<PreviousBatchInfo> inputQueue;

    private static class VmsTracking {
        public final String identifier;
        public long batch;
        public long lastTid;
        public long previousBatch;
        public int numberOfTIDsCurrentBatch;

        private VmsTracking(String identifier) {
            this.identifier = identifier;
        }
    }

    private record PendingTransactionInput (
         long tid, TransactionInput input){}

    public static TransactionWorker build(Deque<TransactionInput> deque, long startingTid, int maxNumberOfTIDsBatch, int batchWindow, int numWorkers, Map<String, TransactionDAG> transactionMap, Map<String, VmsNode[]> vmsIdentifiersPerDAG, Map<String, Coordinator.VmsWorkerContainer> vmsWorkerContainerMap, IVmsSerdesProxy serdesProxy){

        return null;
    }

    private TransactionWorker(Deque<TransactionInput> deque, long startingTid, int maxNumberOfTIDsBatch, int batchWindow, int numWorkers, Map<String, TransactionDAG> transactionMap, Map<String,VmsTracking[]> vmsPerDag, Map<String, Coordinator.VmsWorkerContainer> vmsWorkerContainerMap, IVmsSerdesProxy serdesProxy){
        this.deque = deque;
        this.tid = startingTid;
        this.maxNumberOfTIDsBatch = maxNumberOfTIDsBatch;
        this.batchWindow = batchWindow;
        this.numWorkers = numWorkers;
        this.transactionMap = transactionMap;
        this.vmsWorkerContainerMap = vmsWorkerContainerMap;
        this.serdesProxy = serdesProxy;
        this.vmsPerTransactionMap = new HashMap<>();
        this.vmsTrackingMap = new HashMap<>();
    }

    @Override
    public void run() {
        long end = System.currentTimeMillis() + this.batchWindow;
        TransactionInput data;
        Object msg;
        while (this.isRunning()) {
            // iterate over deques and drain transaction inputs
            do {

                // drain deque
                while ((data = deque.poll()) != null) {
                    this.queueTransactionInput(data);
                }

                /*
                if(pollMessages){
                    while((msg = this.coordinatorQueue.poll()) != null){
                        this.processVmsMessage(msg);
                    }
                }
                */

            } while (System.currentTimeMillis() < end);
            this.advanceCurrentBatch();
            end = System.currentTimeMillis() + this.batchWindow;
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
        boolean markAsPending = false;
        for (var vms_ : vmsList) {
            previousTidPerVms.put(vms_.identifier, vms_.lastTid);
            if(vms_.batch != this.batchContext.batchOffset){
                vms_.previousBatch = vms_.batch;
                vms_.batch = this.batchContext.batchOffset;
                markAsPending = true;
            }
            vms_.lastTid = this.tid;
            vms_.numberOfTIDsCurrentBatch++;
            break;
        }

        batchContext.terminalVMSs.addAll( transactionDAG.terminalNodes );

        if(!markAsPending) {
            String precedenceMapStr = this.serdesProxy.serializeMap(previousTidPerVms);

            TransactionEvent.PayloadRaw txEvent = TransactionEvent.of(this.tid, this.batchContext.batchOffset,
                    transactionInput.event.name, transactionInput.event.payload, precedenceMapStr);

            this.vmsWorkerContainerMap.get(inputVms.identifier).queueTransactionEvent(txEvent);
        }
        else {
            // this has to be emmitted when batch info from previous worker in the ring arrives
            this.pendingInputMap.get(this.batchContext.batchOffset - 1).add(new PendingTransactionInput(this.tid, transactionInput));
            // later on, when receiving the batch info, iterate over the vms of these pending inputs
            // apply the previous batch and last tid, and submit to the vms worker
        }

        this.tid++;
    }

    private void advanceCurrentBatch() {

        if(!this.pendingInputMap.get(this.batchContext.batchOffset - 1).isEmpty()){
            // comes in order
            PreviousBatchInfo previousBatchInfo = this.inputQueue.poll();
            // iterate over VMSs of every pending input, build the previousTidPerVms
            if(previousBatchInfo == null){
                // return since it has not arrived yet
                return;
            }
        }


        startingTid = (startingTid + maxNumberOfTIDsBatch) * numWorkers;


        // iterate over all vms in the last batch, filter those which last tid != this.tid

        // after filtering, send a map containing the vms (identifier) and their corresponding last tids to the next transaction worker in the ring

        this.tid = startingTid;

        // FIXME must save this in the batch context map
        this.batchContext = new BatchContext(this.batchContext.batchOffset+1);

        // build batch precedence map for next worker in the ring


    }

}
