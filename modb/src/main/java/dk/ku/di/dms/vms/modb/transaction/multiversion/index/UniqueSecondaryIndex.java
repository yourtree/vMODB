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

    private final ReadWriteIndex<IKey> underlyingIndex;

    public UniqueSecondaryIndex(ReadWriteIndex<IKey> underlyingIndex) {
        this.underlyingIndex = underlyingIndex;
    }

    @Override
    public void undoTransactionWrites() {

    }

    @Override
    public void installWrites() {

    }

    @Override
    public boolean insert(IKey key, Object[] record) {
        this.underlyingIndex.insert( key, record );
        return true;
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
        if(this.underlyingIndex.exists(key)) {
            return this.underlyingIndex.lookupByKey(key);
        }
        return null;
    }

    public ReadWriteIndex<IKey> underlyingIndex(){
        return this.underlyingIndex;
    }


}
