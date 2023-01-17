package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitRequest;

public class BatchContext {

    public final long batch;

    public final long lastTid;

    private Status status;

    public BatchCommitRequest.Payload requestPayload;

    public BatchContext(long batch, long lastTid) {
        this.batch = batch;
        this.lastTid = lastTid;
        this.status = Status.NEW;
    }

    /**
     * A batch being completed in a VMS does not necessarily mean
     * it can commit the batch. A new leader may need to abort the
     * last batch. In this case, the (local) state must be restored to
     * last logged state.
     */
    public enum Status {
        NEW,
        BATCH_COMPLETED,
        REPLYING_BATCH_COMPLETED, // to the coordinator
        BATCH_COMPLETION_INFORMED, // write has completed
        LOGGING,
        LOGGED,
        REPLYING_BATCH_COMMITTED,
        BATCH_COMMIT_INFORMED
    }

    public Status status(){
        return this.status;
    }

    public void setStatus(Status status){
        this.status = status;
    }

}
