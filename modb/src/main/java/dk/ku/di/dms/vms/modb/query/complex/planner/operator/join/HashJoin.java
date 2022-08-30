package dk.ku.di.dms.vms.modb.query.complex.planner.operator.join;

import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.refac.FilterContext;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.storage.iterator.BucketIterator;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordBucketIterator;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordIterator;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryRefNode;

/**
 * A hash join where its dependencies are fulfilled from the start
 * In other words, previous scanning and transformation steps are not necessary
 */
public class HashJoin extends AbstractJoin {

    public HashJoin(int id, int entrySize,
                    AbstractIndex<IKey> outerIndex, AbstractIndex<IKey> innerIndex,
                    FilterContext filterOuter, FilterContext filterInner) {
        super(id, entrySize, outerIndex, innerIndex, filterOuter, filterInner);
    }

    @Override
    public JoinOperatorTypeEnum getType() {
        return JoinOperatorTypeEnum.HASH;
    }

    public MemoryRefNode run() {

        var leftUnique = leftIndex.getType() == IndexTypeEnum.UNIQUE;
        var rightUnique = rightIndex.getType() == IndexTypeEnum.UNIQUE;

        if(leftUnique && rightUnique){
            return processBothUnique();
        }

        if(leftUnique){
            return processLeftUniqueRightNonUnique();
        }

        if(!rightUnique){
            return processBothNonUnique();
        } else {
            return processLeftNonUniqueRightUnique();
        }

    }

    private MemoryRefNode processLeftNonUniqueRightUnique() {

        BucketIterator outerBucketIterator = leftIndex.asNonUniqueHashIndex().iterator();
        UniqueHashIndex innerIndex = rightIndex.asUniqueHashIndex();

        long outerAddress;
        long innerAddress;

        // for each bucket in inner index
        while(outerBucketIterator.hasNext()) {

            RecordBucketIterator outerRecordIt = outerBucketIterator.next();

            // for each record of the inner bucket
            while (outerRecordIt.hasNext()) {

                outerAddress = outerRecordIt.next();

                if (checkCondition(outerAddress, leftFilter, leftIndex)) {

                    int outerKey = outerRecordIt.key(outerAddress);
                    innerAddress = innerIndex.retrieve(outerKey);
                    if (innerIndex.exists(innerAddress)){

                        if(checkCondition(innerAddress, rightFilter, rightIndex)) {
                            append( outerAddress, innerAddress );
                        }

                    }

                }

            }

        }

        return memoryRefNode;

    }

    private MemoryRefNode processBothNonUnique() {
        BucketIterator outerBucketIterator = leftIndex.asNonUniqueHashIndex().iterator();
        NonUniqueHashIndex innerIndex = rightIndex.asNonUniqueHashIndex();

        long outerAddress;
        long innerAddress;

        // for each bucket in outer index
        while(outerBucketIterator.hasNext()){

            RecordBucketIterator outerRecordIt = outerBucketIterator.next();

            // for each record of the outer bucket
            while(outerRecordIt.hasNext()){

                outerAddress = outerRecordIt.next();

                if(checkCondition(outerAddress, leftFilter, leftIndex )){

                    // assuming the key
                    RecordBucketIterator innerRecordIt = innerIndex.iterator( outerRecordIt.key(outerAddress) );

                    while(innerRecordIt.hasNext()){

                        innerAddress = innerRecordIt.next();
                        if(checkCondition(innerAddress, rightFilter, rightIndex)){
                            append(outerAddress, innerAddress);
                        }

                    }

                }

            }

        }

        return memoryRefNode;
    }

    private MemoryRefNode processLeftUniqueRightNonUnique() {
        RecordIterator outerIterator = leftIndex.asUniqueHashIndex().iterator();
        NonUniqueHashIndex innerIndex = rightIndex.asNonUniqueHashIndex();
        long outerAddress;
        long innerAddress;

        while(outerIterator.hasNext()){

            outerAddress = outerIterator.next();

            if(checkCondition(outerAddress, leftFilter, leftIndex)){

                int outerKey = outerIterator.key(outerAddress);

                // get the iterator
                if(!innerIndex.isBucketEmpty( outerKey )){

                    RecordBucketIterator innerIterator = innerIndex.iterator(outerKey);
                    while(innerIterator.hasNext()){
                        innerAddress = innerIterator.next();
                        if(checkCondition(innerAddress, rightFilter, rightIndex)) {
                            append( outerAddress, innerAddress );
                        }

                    }

                }

            }

        }

        return memoryRefNode;
    }

    private MemoryRefNode processBothUnique() {
        RecordIterator outerIterator = leftIndex.asUniqueHashIndex().iterator();
        UniqueHashIndex innerIndex = rightIndex.asUniqueHashIndex();
        long outerAddress;
        long innerAddress;

        while(outerIterator.hasNext()){

            outerAddress = outerIterator.next();

            if(checkCondition(outerAddress, leftFilter, leftIndex)){

                // no need to materialize the key
                // for secondary index, read the materialized hash
                // for both the PK, if key is single column and if multiple column,
                //  read the materialized field
                int outerKey = outerIterator.key(outerAddress);
                innerAddress = innerIndex.retrieve(outerKey);
                if (innerIndex.exists(innerAddress)){

                    if(checkCondition(innerAddress, rightFilter, rightIndex)) {
                        // append both addresses
                        // structure:
                        // innerSrcAddress, outerSrcAddress
                        append( outerAddress, innerAddress );
                    }

                }

            }

        }

        return memoryRefNode;
    }

}
