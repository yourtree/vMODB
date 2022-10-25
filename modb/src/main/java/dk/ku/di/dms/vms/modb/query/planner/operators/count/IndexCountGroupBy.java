package dk.ku.di.dms.vms.modb.query.planner.operators.count;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyIndex;
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

        IRecordIterator iterator = index.iterator(keys);

        while(iterator.hasElement()){
            if(index.checkCondition(iterator, filterContext)){
                compute(iterator, countMap);
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

    private void compute(IRecordIterator<IKey> iterator, Map<Integer,Integer> countMap) {

        Object record = this.index.record( iterator );

        // hash the groupby columns
        int groupKey = KeyUtils.buildRecordKey( this.indexColumns, record ).hashCode();

        if( countMap.get(groupKey) == null ){
            countMap.put(groupKey, 1);
        } else {
            int newCount = countMap.get(groupKey) + 1;
            countMap.put( groupKey, newCount );
        }
    }

}
