package dk.ku.di.dms.vms.modb.transaction.multiversion;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

/**
 * Only allowed values are exposed as part of the iteration
 */
public class ConsistentRecordIterator implements IRecordIterator {

    private final ReadOnlyIndex<IKey> index;
    private final IRecordIterator recordIterator;

    public ConsistentRecordIterator(ReadOnlyIndex<IKey> index, IRecordIterator recordIterator) {
        this.index = index;
        this.recordIterator = recordIterator;
    }

    @Override
    public boolean hasNext() {
        return recordIterator.hasNext();
    }

    @Override
    public Long next() {
        // additional checks, i.e., the visibility

        // is it visible?
        while( ! this.index.exists( primaryKey() ) ){
            recordIterator.next();
        }

        return recordIterator.current();

    }

    @Override
    public IKey primaryKey() {
        return recordIterator.primaryKey();
    }

    @Override
    public long current() {
        return recordIterator.current();
    }
}
