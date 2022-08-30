package dk.ku.di.dms.vms.modb.query.complex.planner.operator.join;

import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.refac.FilterContext;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordBucketIterator;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordIterator;

/**
 * The nested loop is used when there is no possible index for the
 * intended join operation.
 *
 * In the indexed nested loop, the outer is iterated and probes the inner.
 * The inner may be a unique or non unique.
 *
 * The outer is always the PK, thus a {@link UniqueHashIndex}
 */
public class IndexNestedLoopJoin extends AbstractJoin {

    private final int[] outerColumns;

    public IndexNestedLoopJoin(int id, int entrySize,
                               UniqueHashIndex outerIndex, int[] outerColumns,
                               AbstractIndex<IKey> innerIndex,
                               FilterContext filterOuter, FilterContext filterInner) {
        super(id, entrySize, outerIndex, innerIndex, filterOuter, filterInner);
        this.outerColumns = outerColumns;
    }

    @Override
    public JoinOperatorTypeEnum getType() {
        return JoinOperatorTypeEnum.INDEX_NESTED_LOOP;
    }

    public MemoryRefNode run() {

        RecordIterator outerIterator = leftIndex.asUniqueHashIndex().iterator();
        var rightUnique = rightIndex.getType() == IndexTypeEnum.UNIQUE;

        long outerAddress;
        long innerAddress;

        // right is looked up
        if(rightUnique) {

            UniqueHashIndex innerIndex = rightIndex.asUniqueHashIndex();

            while (outerIterator.hasNext()) {

                outerAddress = outerIterator.next();

                if (checkCondition(outerAddress, leftFilter, leftIndex)) {

                    // this key is not materialized in the outer, but in the inner
                    IKey probeKey = KeyUtils.buildRecordKey( leftIndex.getTable().getSchema(), outerColumns, outerAddress );

                    innerAddress = innerIndex.retrieve(probeKey);

                    if(innerIndex.exists(innerAddress)){

                        if(checkCondition(innerAddress, rightFilter, rightIndex))
                            append(outerAddress, innerAddress);
                    }

                }

            }

            return memoryRefNode;
        }

        NonUniqueHashIndex innerIndex = rightIndex.asNonUniqueHashIndex();
        RecordBucketIterator innerIterator;

        while (outerIterator.hasNext()) {

            outerAddress = outerIterator.next();

            if (checkCondition(outerAddress, leftFilter, leftIndex)) {

                // this key is not materialized in the outer, but in the inner
                IKey probeKey = KeyUtils.buildRecordKey( leftIndex.getTable().getSchema(), outerColumns, outerAddress );

                // get the iterator
                if(!innerIndex.isBucketEmpty( probeKey.hashCode() )){

                    innerIterator = innerIndex.iterator(probeKey.hashCode());
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

}
