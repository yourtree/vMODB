package dk.ku.di.dms.vms.sdk.core.operational;

import dk.ku.di.dms.vms.modb.api.enums.ExecutionModeEnum;
import dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionalHandler;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * Given the transactional handler and callback are shared among all instances
 * of transaction tasks, the builder offers these to avoid
 * repetitive creation of these attributes in task instances
 */
public final class VmsTransactionTaskBuilder {

    private static final Logger logger = Logger.getLogger(VmsTransactionTask.class.getSimpleName());

    private static final int NEW = 0;
    private static final int READY = 1;
    private static final int RUNNING = 2;
    private static final int FINISHED = 3;

    private final ITransactionalHandler transactionalHandler;

    private final ISchedulerCallback schedulerCallback;

    public VmsTransactionTaskBuilder(ITransactionalHandler transactionalHandler,
                                    ISchedulerCallback schedulerCallback) {
        this.transactionalHandler = transactionalHandler;
        this.schedulerCallback = schedulerCallback;
    }

    public final class VmsTransactionTask implements Runnable {

        private final long tid;

        private final long lastTid;

        private final long batch;

        // the information necessary to run the method
        private final VmsTransactionSignature signature;

        private final Object input;

        private volatile int status;

        private final Optional<Object> partitionId;

        private VmsTransactionTask(long tid, long lastTid, long batch,
                                   VmsTransactionSignature signature,
                                   Object input) {
            this.tid = tid;
            this.lastTid = lastTid;
            this.batch = batch;
            this.signature = signature;
            this.input = input;
            Optional<Object> partitionIdAux;
            try {
                if (signature.executionMode() == ExecutionModeEnum.PARTITIONED) {
                    partitionIdAux = Optional.of(signature.partitionByMethod().invoke(input()));
                } else {
                    partitionIdAux = Optional.empty();
                }
            } catch (InvocationTargetException | IllegalAccessException e){
                logger.warning("Failed to obtain partition key from method "+signature.partitionByMethod().getName());
                partitionIdAux = Optional.empty();
            }
            this.partitionId = partitionIdAux;
        }

        @Override
        public void run() {

            this.status = RUNNING;

            transactionalHandler.beginTransaction(this.tid, -1, this.lastTid, this.signature.transactionType() == TransactionTypeEnum.R);

            try {
                Object output = this.signature.method().invoke(this.signature.vmsInstance(), this.input);

                OutboundEventResult eventOutput = new OutboundEventResult(this.tid, this.batch, this.signature.outputQueue(), output);

                if(this.signature.transactionType() != TransactionTypeEnum.R){
                    transactionalHandler.commit();
                }

                this.status = FINISHED;
                schedulerCallback.success(signature.executionMode(), eventOutput);
            } catch (Exception e) {
                schedulerCallback.error(signature.executionMode(), this.tid, e);
            }

        }

        public long tid() {
            return this.tid;
        }

        public long lastTid() {
            return this.lastTid;
        }

        public VmsTransactionSignature signature(){
            return this.signature;
        }

        public Object input(){
            return input;
        }

        public int status(){
            return this.status;
        }

        // set as ready for execution. the thread pool is able to execute it
        public void signalReady(){
            this.status = READY;
        }

        public boolean isScheduled(){
            return this.status > NEW;
        }

        public Optional<Object> partitionId() {
            return partitionId;
        }

    }

    public VmsTransactionTask build(long tid, long lastTid, long batch,
                                    VmsTransactionSignature signature,
                                    Object input){
        return new VmsTransactionTask(tid, lastTid, batch, signature, input);
    }

    public VmsTransactionTask buildFinished(long tid){
        var sig = new VmsTransactionSignature(null, null, null, ExecutionModeEnum.SINGLE_THREADED, null, null);
        var deadTask = new VmsTransactionTask(tid, 0, 0, sig, null);
        deadTask.status = FINISHED;
        return deadTask;
    }

}