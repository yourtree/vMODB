package dk.ku.di.dms.vms.modb.common.event;

/**
 * It represents the result of a request for data
 */
public class DataResponseEvent {

    // request identifier. the thread id
    public long identifier;

    public Object result;

    public DataResponseEvent(long identifier, Object result) {
        this.identifier = identifier;
        this.result = result;
    }
}
