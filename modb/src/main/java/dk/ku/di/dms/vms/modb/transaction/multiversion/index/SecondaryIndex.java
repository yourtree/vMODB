package dk.ku.di.dms.vms.modb.transaction.multiversion.index;

import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.ReadOnlyIndex;

public class SecondaryIndex implements ReadOnlyIndex<IKey> {

    // pointer to primary index
    // necessary because of transaction, concurrency control
    private final PrimaryIndex primaryIndex;

    // can be a unique on non-unique hash index
    private final ReadOnlyIndex<IKey> secondaryIndex;

    public SecondaryIndex(PrimaryIndex primaryIndex, ReadOnlyIndex<IKey> secondaryIndex) {
        this.primaryIndex = primaryIndex;
        this.secondaryIndex = secondaryIndex;
    }

    @Override
    public IIndexKey key() {
        return this.secondaryIndex.key();
    }

    @Override
    public Schema schema() {
        return this.primaryIndex.schema();
    }

    @Override
    public int[] columns() {
        return this.secondaryIndex.columns();
    }

    @Override
    public boolean containsColumn(int columnPos) {
        return this.secondaryIndex.containsColumn(columnPos);
    }

    @Override
    public IndexTypeEnum getType() {
        return this.secondaryIndex.getType();
    }

    @Override
    public boolean exists(IKey key) {

        // find record in secondary index

        // retrieve PK from record

        // make sure record exists in PK

        // if not, delete from sec idx and return false

        // if so, return yes

        return this.secondaryIndex.exists(key);
    }

    @Override
    public boolean exists(long address) {
        return false;
    }

    @Override
    public long retrieve(IKey key) {
        return 0;
    }

}
