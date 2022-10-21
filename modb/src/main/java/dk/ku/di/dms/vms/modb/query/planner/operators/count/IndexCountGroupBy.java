package dk.ku.di.dms.vms.modb.query.planner.operators.count;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class IndexCountGroupBy extends AbstractCount {

    // the columns declared in the group by clause
    private final int[] indexColumns;

    public IndexCountGroupBy(ReadOnlyIndex<IKey> index,
                             int[] indexColumns) {
        super(index, Integer.BYTES);
        this.indexColumns = indexColumns;
    }

    public MemoryRefNode run(FilterContext filterContext, IKey... keys){

        Map<Integer,Integer> countMap = new HashMap<>();

        if(index.getType() == IndexTypeEnum.UNIQUE){

            UniqueHashIndex cIndex = index.asUniqueHashIndex();
            long address;
            for(IKey key : keys){
                address = cIndex.retrieve(key);
                if(index.checkCondition(key, address, filterContext)){
                    compute(key, address, countMap);
                }
            }

            // append(countMap);
            return memoryRefNode;

        }

        // non unique
        NonUniqueHashIndex cIndex = index.asNonUniqueHashIndex();
        for(IKey key : keys){
            IRecordIterator iterator = cIndex.iterator(key);
            while(iterator.hasNext()){

                if(index.checkCondition(iterator, filterContext)){
                    compute(iterator, countMap);
                }

                iterator.next();

            }
        }

        append(countMap);
        return memoryRefNode;

    }

    private void append(Map<Integer, Integer> countMap) {
        ensureMemoryCapacity(this.entrySize * countMap.size());

        // number of "rows"
        this.currentBuffer.append(countMap.size());

        countMap.forEach((key, value) -> {
            this.currentBuffer.append(key);
            this.currentBuffer.append(value);
        });

    }

    private void compute(IKey key, long address, Map<Integer,Integer> countMap) {
        int groupKey = index.hashAggregateGroup(key, address, indexColumns).hashCode();

        if( countMap.get(groupKey) == null ){
            countMap.put(groupKey, 1);
        } else {
            int newCount = countMap.get(groupKey) + 1;
            countMap.put( groupKey, newCount );
        }
    }

    private void compute(IRecordIterator iterator, Map<Integer,Integer> countMap) {

        // hash the groupby columns
        int groupKey = index.hashAggregateGroup(iterator, indexColumns).hashCode();

        if( countMap.get(groupKey) == null ){
            countMap.put(groupKey, 1);
        } else {
            int newCount = countMap.get(groupKey) + 1;
            countMap.put( groupKey, newCount );
        }
    }

}
