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

    public enum Status {
        NEW,
        COMPLETED,
        REPLYING_COMPLETED, // to the coordinator
        COMPLETION_INFORMED, // write has completed
        LOGGING,
        COMMITTED,
        REPLYING_COMMITTED,
        COMMIT_INFORMED
    }

    public Status status(){
        return this.status;
    }

    public void setStatus(Status status){
        this.status = status;
    }

}
