package dk.ku.di.dms.vms.event;

/**
 * Every event that triggers the execution of data operations must implement the
 * data attributes contained in this class, so the scheduler can reason about the
 * correct order
 */
public abstract class TransactionalEvent implements IEvent {

    // transaction id
    public int tid;

    // cached queue name for faster key-value search
    public String source;

}
