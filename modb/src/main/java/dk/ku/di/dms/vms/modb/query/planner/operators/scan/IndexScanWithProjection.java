package dk.ku.di.dms.vms.modb.query.planner.operators.scan;

import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordBucketIterator;
import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;

/**
 * On-flight scanning, filtering, and projection in a single operator.
 *
 * The caller knows the output format.
 * But called does not know how many records are.
 *
 * Header:
 * int - number of rows returned
 *
 * This must be performed by a proper method at the end of the procedure
 * We can have a method called seal or close() in abstract operator
 * It will put the information on header.
 *
 */
public final class IndexScanWithProjection extends AbstractScan {

    public IndexScanWithProjection(
                     Table table,
                     ReadOnlyIndex<IKey> index,
                     int[] projectionColumns,
                     int entrySize) {
        super(table, entrySize, index, projectionColumns);
    }

    @Override
    public boolean isIndexScan() {
        return true;
    }

    @Override
    public IndexScanWithProjection asIndexScan() {
        return this;
    }

    public MemoryRefNode run(FilterContext filterContext, IKey... keys) {

        if(index.getType() == IndexTypeEnum.UNIQUE){
            long address;
            UniqueHashIndex cIndex = index.asUniqueHashIndex();
            for(IKey key : keys){
                address = index.retrieve(key);
                if(cIndex.checkCondition(key, address, filterContext)){
                    append(key, address, projectionColumns);
                }
            }

            return memoryRefNode;

        }

        // non unique
        NonUniqueHashIndex cIndex = index.asNonUniqueHashIndex();
        for(IKey key : keys){
            RecordBucketIterator iterator = cIndex.iterator(key);
            while(iterator.hasNext()){
                if(cIndex.checkCondition(iterator, filterContext)){
                    append(iterator, projectionColumns);
                }
                // move the iterator
                iterator.next();
            }
        }

        return memoryRefNode;
    }

}