package dk.ku.di.dms.vms.modb.transaction.multiversion.operation;

public class InsertOp extends DataItemVersion {

    // to copy the values to the query result
    public long bufferAddress;

    protected InsertOp(long tid, long bufferAddress) {
        super(tid);
        this.bufferAddress = bufferAddress;
    }

}
