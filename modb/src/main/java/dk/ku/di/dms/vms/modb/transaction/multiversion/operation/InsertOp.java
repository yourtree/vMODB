package dk.ku.di.dms.vms.modb.transaction.multiversion.operation;

public class InsertOp extends DataItemVersion {

    // only filled if it is insert
    // private ByteBuffer buffer;

    // to copy the values to the query result
    public long bufferAddress;

    protected InsertOp(long tid, long bufferAddress) {
        super(tid);
        this.bufferAddress = bufferAddress;
    }

}
