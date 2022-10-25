package dk.ku.di.dms.vms.modb.storage.iterator.non_unique;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

/**
 * Encapsulates iteration over buckets and
 * respective records, thus refraining the operators
 * to handle that.
 * TODO finish
 */
public class NonUniqueRecordIterator implements IRecordIterator<IKey> {

    private final BucketIterator bucketIterator;

    public NonUniqueRecordIterator(BucketIterator bucketIterator){
        this.bucketIterator = bucketIterator;
    }

    @Override
    public IKey get() {
        return null;
    }

    @Override
    public boolean hasElement() {
        return false;
    }

    @Override
    public void next() {
        return;
    }

}
