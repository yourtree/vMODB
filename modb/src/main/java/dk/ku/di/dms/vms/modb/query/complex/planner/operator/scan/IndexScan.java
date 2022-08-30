package dk.ku.di.dms.vms.modb.query.complex.planner.operator.scan;

import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.refac.FilterContext;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordBucketIterator;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryRefNode;

/**
 * Receives a key, performs the lookup, and apply the additional filters
 *
 * Supports only...
 *
 *
 * planner checks the condition (where clause)
 * is there any index might be applied?
 *
 * table A a1,a2,a3
 *
 * index 1 = a1
 * index 2 = a1, a2
 *
 * where A.a1 = 0 and A.a2 = 1
 *
 * pick index 2
 *
 * planner will form a key composed by <a1,a2>
 *
 * this key is passed to the index scan, the index scan queries the index looking for the record or set of records
 * if the index is unique hash , only a record is returned
 * if index is non unique, multiple records may be returned
 *
 */
public final class IndexScan extends AbstractScan {

    private final IKey[] keys;

    public IndexScan(int id, AbstractIndex<IKey> index, FilterContext filterContext, IKey... keys) {
        super(id, index, filterContext);
        this.keys = keys;
    }

    public MemoryRefNode run() {

        if(index.getType() == IndexTypeEnum.UNIQUE){

            UniqueHashIndex cIndex = index.asUniqueHashIndex();
            long address;
            for(IKey key : keys){
                address = cIndex.retrieve(key);
                if(checkCondition(address, filterContext, index)){
                    append(address);
                }
            }

            return memoryRefNode;

        }

        // non unique
        NonUniqueHashIndex cIndex = index.asNonUniqueHashIndex();
        for(IKey key : keys){
            RecordBucketIterator iterator = cIndex.iterator(key);
            processIterator(iterator);
        }

        return memoryRefNode;
    }

}
