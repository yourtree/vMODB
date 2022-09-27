package dk.ku.di.dms.vms.modb.transaction.multiversion.operation;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IIndexKey;

public class UpdateOp extends DataItemVersion {

    /** two below fields are ony filled if it is update */
    // a transaction can make changes to several data items
    // here the granularity is of a column
    public int columnIndex;

    // the value
    public final long address;

    protected UpdateOp(long tid, int columnIndex, long address, IIndexKey indexKey, IKey pk) {
        super(tid, indexKey, pk);
        this.columnIndex = columnIndex;
        this.address = address;
    }

    /**
     * For cases where multiple columns are updated, makes no sense to fine track
     */
    protected UpdateOp(long tid, long address, IIndexKey indexKey, IKey pk) {
        super(tid, indexKey, pk);
        this.address = address;
    }

}
