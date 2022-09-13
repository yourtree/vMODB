package dk.ku.di.dms.vms.modb.common.event;

/**
 * It represents a request for data (i.e., triggers a query in the sidecar)
 * or the issuing of an insert or update in the sidecar
 */
public class DataRequestEvent {

    // request identifier. the thread id
    public long identifier;

    public String type; // select, update, insert, delete

    // json
    public String statement;

    public boolean isDTO;

    public DataRequestEvent(long identifier, String type, String statement, boolean isDTO) {
        this.identifier = identifier;
        this.type = type;
        this.statement = statement;
        this.isDTO = isDTO;
    }

}
