package dk.ku.di.dms.vms.modb.transaction.multiversion.index;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;

/**
 * The same key from PK is used to find records in this index
 * but only a portion of the data from the primary index is
 * found here. The criteria is given via annotating
 * {@link dk.ku.di.dms.vms.modb.api.annotations.VmsPartialIndex}
 * in a column belonging to a {@link VmsTable}.
 */
public final class UniqueSecondaryIndex implements IMultiVersionIndex {

    private final ReadWriteIndex<IKey> primaryKeyIndex;

    public UniqueSecondaryIndex(ReadWriteIndex<IKey> primaryKeyIndex) {
        this.primaryKeyIndex = primaryKeyIndex;
    }

    @Override
    public void undoTransactionWrites() {

    }

    @Override
    public void installWrites() {

    }

    @Override
    public boolean insert(IKey key, Object[] record) {
        return false;
    }

    @Override
    public boolean update(IKey key, Object[] record) {
        return false;
    }

    @Override
    public boolean remove(IKey key) {
        return false;
    }

    @Override
    public Object[] lookupByKey(IKey key){
        if(this.primaryKeyIndex.exists(key)) {
            return this.primaryKeyIndex.lookupByKey(key);
        }
        return null;
    }

    public ReadWriteIndex<IKey> underlyingIndex(){
        return this.primaryKeyIndex;
    }



}
