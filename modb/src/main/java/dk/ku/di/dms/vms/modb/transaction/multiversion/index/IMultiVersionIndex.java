package dk.ku.di.dms.vms.modb.transaction.multiversion.index;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

public interface IMultiVersionIndex extends ReadOnlyIndex<IKey> {

    void undoTransactionWrites();

    void installWrites();

    boolean insert(IKey key, Object[] record);

    boolean update(IKey key, Object[] record);

    IRecordIterator<IKey> EMPTY_ITERATOR = new IRecordIterator<>() {
        @Override
        public IKey get() {
            return null;
        }
        @Override
        public void next() { }
        @Override
        public boolean hasElement() {
            return false;
        }
    };

}
