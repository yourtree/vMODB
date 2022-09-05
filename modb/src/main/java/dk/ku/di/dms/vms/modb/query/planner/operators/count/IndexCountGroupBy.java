package dk.ku.di.dms.vms.modb.query.planner.operators.count;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractOperator;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordBucketIterator;
import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class IndexCountGroupBy extends AbstractOperator {

    private final AbstractIndex<IKey> index;

    private final int[] indexColumns;

    private final Map<int,Integer> countMap;

    public IndexCountGroupBy(AbstractIndex<IKey> index,
                             int[] indexColumns) {
        super(Integer.BYTES);
        this.index = index;
        this.indexColumns = indexColumns;
        this.countMap = new HashMap<int,Integer>();
    }

    public MemoryRefNode run(FilterContext filterContext, IKey... keys){

        if(index.getType() == IndexTypeEnum.UNIQUE){

            UniqueHashIndex cIndex = index.asUniqueHashIndex();
            long address;
            for(IKey key : keys){
                address = cIndex.retrieve(key);
                if(checkCondition(address, filterContext, index.getTable().getSchema())){
                    compute(address);
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

                address = iterator.next();

                if(checkCondition(address, filterContext, index.getTable().getSchema())){
                    compute(address);
                }

            }
        }

        // append(countMap);
        return memoryRefNode;

    }

    private void compute(long address) {
        // hash the groupby columns
        int groupKey = KeyUtils.buildRecordKey(  this.index.getTable().getSchema(), indexColumns, address).hashCode();
        if( countMap.get(groupKey) == null ){
            countMap.put(groupKey, 1);
        } else {
            int newCount = countMap.get(groupKey) + 1;
            countMap.put( groupKey, newCount );
        }
    }

}
