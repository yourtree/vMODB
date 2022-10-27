package dk.ku.di.dms.vms.modb.storage.iterator.non_unique;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

/**
 * Encapsulates iteration over buckets and
 * respective records, thus refraining the
 * operators to handle that.
 */
public class NonUniqueRecordIterator implements IRecordIterator<IKey> {

    private final BucketIterator bucketIterator;

    private RecordBucketIterator currentBucket;

    public NonUniqueRecordIterator(BucketIterator bucketIterator){
        this.bucketIterator = bucketIterator;
        // error prevention in the future =) just believe there are buckets and records in the index
        // if(bucketIterator.hasNext())
            this.currentBucket = bucketIterator.next();
    }

    @Override
    public IKey get() {
        return currentBucket.key();
    }

    @Override
    public boolean hasElement() {

        if(currentBucket.hasElement()) return true;

        if( bucketIterator.hasNext() ){
            // move to another bucket
            currentBucket = bucketIterator.next();
        }

        // the next bucket may have no element
        return currentBucket.hasElement();
    }

    @Override
    public void next() {
        currentBucket.next();
    }

}
