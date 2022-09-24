package dk.ku.di.dms.vms.sdk.embed;

public class BatchContext {

    public final long batch;

    public final long lastTid;

    private Status status;

    public BatchContext(long batch, long lastTid) {
        this.batch = batch;
        this.lastTid = lastTid;
        this.status = Status.NEW;
    }

    enum Status {
        NEW,
        COMMITTING,
        FINISHED
    }

    public Status status(){
        return this.status;
    }

    public void setStatus(Status status){
        this.status = status;
    }

}
