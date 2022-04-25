package dk.ku.di.dms.vms.modb.common.event;

import dk.ku.di.dms.vms.modb.common.query.statement.IStatement;

/**
 * It represents a request for data (i.e., triggers a query in the sidecar)
 * or the issuing of an insert or update in the sidecar
 */
public class DataRequestEvent implements IVmsEvent {

    // request identifier. the thread id
    public long identifier;

    public String type; // select, update, insert, delete

    public IStatement statement;

    public boolean isDTO;

    public DataRequestEvent(long identifier, String type, IStatement statement, boolean isDTO) {
        this.identifier = identifier;
        this.type = type;
        this.statement = statement;
        this.isDTO = isDTO;
    }

}
