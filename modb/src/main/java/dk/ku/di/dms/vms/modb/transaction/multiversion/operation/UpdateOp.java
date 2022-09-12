package dk.ku.di.dms.vms.modb.transaction.multiversion.operation;

public class UpdateOp extends DataItemVersion {

    /** two below fields are ony filled if it is update */
    // a transaction can make changes to several data items
    // here the granularity is of a column
    public final int columnIndex;

    // the value
    public final long address;

    protected UpdateOp(long tid, int columnIndex, long address) {
        super(tid);
        this.columnIndex = columnIndex;
        this.address = address;
    }

}
