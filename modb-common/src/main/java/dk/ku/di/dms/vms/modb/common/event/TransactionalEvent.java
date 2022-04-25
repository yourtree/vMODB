package dk.ku.di.dms.vms.modb.common.event;

/**
 * This is the base class for representing the data transferred across the framework and the sidecar
 * It serves both for input and output
 */
public class TransactionalEvent implements IVmsEvent {

    public int tid;
    public String queue; // the queue naturally maps to a type
    public IApplicationEvent payload; // the event itself

    public int tid() {
        return tid;
    }

    public String queue() {
        return queue;
    }

    public IApplicationEvent payload() {
        return payload;
    }

    public TransactionalEvent(int tid, String queue, IApplicationEvent payload) {
        this.tid = tid;
        this.queue = queue;
        this.payload = payload;
    }

}