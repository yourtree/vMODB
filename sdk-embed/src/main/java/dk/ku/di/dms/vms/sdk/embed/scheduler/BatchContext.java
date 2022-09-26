package dk.ku.di.dms.vms.sdk.embed.scheduler;

public class BatchContext {

    public final long batch;

    public final long lastTid;

    private Status status;

    public BatchContext(long batch, long lastTid) {
        this.batch = batch;
        this.lastTid = lastTid;
        this.status = Status.NEW;
    }

    public enum Status {
        NEW,
        IN_PROGRESS,
        COMMITTED,
        REPLYING, // to the coordinator
        DONE // write has completed
    }

    public Status status(){
        return this.status;
    }

    public void setStatus(Status status){
        this.status = status;
    }

}
