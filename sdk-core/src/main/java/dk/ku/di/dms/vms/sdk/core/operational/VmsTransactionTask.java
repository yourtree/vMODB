package dk.ku.di.dms.vms.sdk.core.operational;

import dk.ku.di.dms.vms.modb.api.enums.ExecutionModeEnum;
import dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionalHandler;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import java.util.logging.Logger;

public final class VmsTransactionTask implements Runnable {

    private static final Logger logger = Logger.getLogger(VmsTransactionTask.class.getSimpleName());

    private final long tid;

    private final long lastTid;

    private final long batch;

    private final ITransactionalHandler transactionalHandler;

    private final ISchedulerCallback schedulerCallback;

    // the information necessary to run the method
    private final VmsTransactionSignature signature;

    private final Object input;

    private volatile Status status;

    private final Optional<Object> partitionId;

    public VmsTransactionTask(ITransactionalHandler transactionalHandler,
                              ISchedulerCallback schedulerCallback,
                              long tid, long lastTid, long batch,
                              VmsTransactionSignature signature,
                              Object input) {

        this.transactionalHandler = transactionalHandler;
        this.schedulerCallback = schedulerCallback;
        this.tid = tid;
        this.lastTid = lastTid;
        this.batch = batch;
        this.signature = signature;
        this.input = input;
        this.status = Status.NEW;

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

    public VmsTransactionTask(long tid){
        this.transactionalHandler = null;
        this.schedulerCallback = null;
        this.tid = tid;
        this.lastTid = 0;
        this.batch = 0;
        this.signature = null;
        this.input = null;
        this.status = Status.OUTPUT_SENT;
        this.partitionId = Optional.empty();
    }

    public enum Status {
        NEW(1),
        READY(2),
        RUNNING(3),
        FINISHED(4),
        OUTPUT_SENT(5);

        private final int value;

        Status(int value) {
            this.value = value;
        }

        public int value() {
            return this.value;
        }
    }

    @Override
    public void run() {

        this.status = Status.RUNNING;

        this.transactionalHandler.beginTransaction(this.tid, -1, this.lastTid, this.signature.transactionType() == TransactionTypeEnum.R);

        try {
            Object output = this.signature.method().invoke(this.signature.vmsInstance(), this.input);

            OutboundEventResult eventOutput = new OutboundEventResult(this.tid, this.batch, this.signature.outputQueue(), output);

            if(this.signature.transactionType() != TransactionTypeEnum.R){
                this.transactionalHandler.commit();
            }

            this.status = Status.FINISHED;
            this.schedulerCallback.success(signature.executionMode(), eventOutput);
        } catch (Exception e) {
            this.schedulerCallback.error(signature.executionMode(), this.tid, e);
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

    public Status status(){
        return this.status;
    }

    // set as ready for execution. the thread pool is able to execute it
    public void signalReady(){
        this.status = Status.READY;
    }

    public void signalOutputSent(){
        this.status = Status.OUTPUT_SENT;
    }

    public Optional<Object> partitionId() {
        return partitionId;
    }

}