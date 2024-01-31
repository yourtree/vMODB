package dk.ku.di.dms.vms.modb.query.execution.operators.min;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.query.execution.operators.scan.AbstractScan;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public final class IndexGroupByMinWithProjection extends AbstractScan {

    private final ReadWriteIndex<IKey> index;

    private final int[] indexColumns;

    private final int[] projectionColumns;

    private final int minColumn;

    private final int limit;

    public IndexGroupByMinWithProjection(ReadWriteIndex<IKey> index,
                                         int[] indexColumns,
                                         int[] projectionColumns,
                                         int minColumn,
                                         int entrySize,
                                         int limit) {
        super(entrySize, index, projectionColumns);
        this.index = index;
        this.indexColumns = indexColumns;
        this.projectionColumns = projectionColumns;
        this.minColumn = minColumn;
        this.limit = limit;
    }

    public MemoryRefNode run(){

        this.ensureMemoryCapacity(this.entrySize * this.limit);

        Map<GroupByKey, Comparable<?>> minMap = new HashMap<>();

        Iterator<IKey> iterator = this.index.iterator();

        // build hash with min per group (defined in group by)
        while(iterator.hasNext()){
            this.compute(iterator.next(), minMap);
        }

        // TODO order by minColumn ... isn't it better to build a tree map ordered then just extract the 10 first later?

        // apply limit while projecting columns
        this.project(minMap);

        return this.memoryRefNode;

    }

    private void project(Map<GroupByKey, Comparable<?>> minMap){
        int i = 0;
        for(var entry : minMap.entrySet()){
            this.append(entry.getKey());
            i++;
            if(i == this.limit) break;
        }
    }

    private void append(GroupByKey key) {
        Object[] record = this.index.record( key.getPk() );
        for (int projectionColumn : this.projectionColumns) {
            DataTypeUtils.callWriteFunction(this.currentBuffer.nextOffset(), this.index.schema().columnDataType(projectionColumn), record[projectionColumn]);
            this.currentBuffer.forwardOffset(this.index.schema().columnDataType(projectionColumn).value);
        }
    }

    private void compute(IKey key, Map<GroupByKey, Comparable<?>> minMap) {

        // TODO materialize only the necessary columns
        Object[] record = this.index.record( key );

        // hash the group by columns
        IKey pk = KeyUtils.buildRecordKey(this.index.schema().getPrimaryKeyColumns(), record);
        IKey groupByKey = KeyUtils.buildRecordKey(this.indexColumns, record );
        GroupByKey groupKey = new GroupByKey( groupByKey.hashCode(), pk );

        if(!minMap.containsKey(groupKey)){
            minMap.put(groupKey, (Comparable<?>) record[minColumn]);
        } else {
            Comparable currVal = minMap.get(groupKey);
            if (currVal.compareTo(record[minColumn]) > 0){
                minMap.put(groupKey, (Comparable<?>) record[minColumn]);
            }
        }
    }

    @Override
    public boolean isIndexAggregationScan(){
        return true;
    }

    public IndexGroupByMinWithProjection asIndexAggregationScan(){
        return this;
    }

}
