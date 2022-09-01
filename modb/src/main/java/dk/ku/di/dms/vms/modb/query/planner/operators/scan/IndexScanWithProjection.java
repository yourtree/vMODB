package dk.ku.di.dms.vms.modb.query.planner.operators.scan;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordBucketIterator;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryRefNode;

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
                     AbstractIndex<IKey> index,
                     int[] projectionColumns,
                     int[] projectionColumnSize, // in bytes
                     int entrySize) {
        super(entrySize, index, projectionColumns, projectionColumnSize);
    }

    @Override
    public boolean isIndexScan() {
        return true;
    }

    @Override
    public IndexScanWithProjection asIndexScan() {
        return this;
    }

    public MemoryRefNode run(IKey[] keys, FilterContext filterContext) {

        if(index.getType() == IndexTypeEnum.UNIQUE){

            UniqueHashIndex cIndex = index.asUniqueHashIndex();
            long address;
            for(IKey key : keys){
                address = cIndex.retrieve(key);
                if(checkCondition(address, filterContext, index)){
                    append(address, projectionColumns, index.getTable().getSchema().columnOffset(), projectionColumnSize);
                }
            }

            return memoryRefNode;

        }

        // non unique
        NonUniqueHashIndex cIndex = index.asNonUniqueHashIndex();
        long address;
        for(IKey key : keys){
            RecordBucketIterator iterator = cIndex.iterator(key);
            while(iterator.hasNext()){

                address = iterator.next();

                if(checkCondition(address, filterContext, index)){
                    append(address, projectionColumns, index.getTable().getSchema().columnOffset(), projectionColumnSize);
                }

            }
        }

        return memoryRefNode;
    }

}