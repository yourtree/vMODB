package dk.ku.di.dms.vms.modb.query.planner.operators.count;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordBucketIterator;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class IndexCountGroupBy extends AbstractCount {

    private final int[] indexColumns;

    public IndexCountGroupBy(ReadOnlyIndex<IKey> index,
                             int[] indexColumns) {
        super(index, Integer.BYTES);
        this.indexColumns = indexColumns;
    }

    public MemoryRefNode run(FilterContext filterContext, IKey... keys){

        Map<int,Integer> countMap = new HashMap<int,Integer>();

        if(index.getType() == IndexTypeEnum.UNIQUE){

            UniqueHashIndex cIndex = index.asUniqueHashIndex();
            long address;
            for(IKey key : keys){
                address = cIndex.retrieve(key);
                if(index.checkCondition(key, filterContext)){
                    compute(address, countMap);
                }
            }

            // append(countMap);
            return memoryRefNode;

        }

        // non unique
        NonUniqueHashIndex cIndex = index.asNonUniqueHashIndex();
        long address;
        for(IKey key : keys){
            RecordBucketIterator iterator = cIndex.iterator(key);
            while(iterator.hasNext()){

                if(index.checkCondition(iterator, filterContext)){
                    address = iterator.current();
                    compute(address, countMap);
                }

            }
        }

        // append(countMap);
        return memoryRefNode;

    }

    private void compute(long address, Map<int,Integer> countMap) {
        // hash the groupby columns
        // TODO this should be the index
        int groupKey = KeyUtils.buildRecordKey(this.index.schema(), indexColumns, address).hashCode();
        if( countMap.get(groupKey) == null ){
            countMap.put(groupKey, 1);
        } else {
            int newCount = countMap.get(groupKey) + 1;
            countMap.put( groupKey, newCount );
        }
    }

}
