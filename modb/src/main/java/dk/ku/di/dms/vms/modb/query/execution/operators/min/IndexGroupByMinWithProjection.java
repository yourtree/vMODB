package dk.ku.di.dms.vms.modb.query.execution.operators.min;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.query.execution.operators.scan.AbstractScan;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.*;

public final class IndexGroupByMinWithProjection extends AbstractScan {

    private final int[] indexColumns;

    private final int[] projectionColumns;

    private final int minColumn;

    private final int limit;

    private final Schema schema;

    public IndexGroupByMinWithProjection(IMultiVersionIndex index,
                                         Schema schema,
                                         int[] indexColumns,
                                         int[] projectionColumns,
                                         int minColumn,
                                         int entrySize,
                                         int limit) {
        super(entrySize, index, projectionColumns);
        this.schema = schema;
        this.indexColumns = indexColumns;
        this.projectionColumns = projectionColumns;
        this.minColumn = minColumn;
        this.limit = limit;
    }

    public List<Object[]> runAsEmbedded(){
        Map<GroupByKey, Comparable<?>> minMap = new HashMap<>();
        Iterator<Object[]> iterator = this.index.iterator();
        // build hash with min per group (defined in group by)
        while(iterator.hasNext()){
            this.compute(iterator.next(), minMap);
        }
        return project(minMap);
    }

//    public MemoryRefNode run(){
//
//        this.ensureMemoryCapacity(this.entrySize * this.limit);
//
//        Map<GroupByKey, Comparable<?>> minMap = new HashMap<>();
//
//        Iterator<IKey> iterator = this.index.iterator();
//
//        // build hash with min per group (defined in group by)
//        while(iterator.hasNext()){
//            TODO materialize only the necessary columns
//            Object[] record = this.index.lookupByKey(iterator.next());
//            this.compute(record, minMap);
//        }
//
//        // TODO order by minColumn ... isn't it better to build a tree map ordered then just extract the 10 first later?
//
//        // apply limit while projecting columns
//        this.project(minMap);
//
//        return this.memoryRefNode;
//
//    }

    private List<Object[]> project(Map<GroupByKey, Comparable<?>> minMap){
        int i = 0;
        int j;
        List<Object[]> result = new ArrayList<>();
        for(var entry : minMap.entrySet()){
            Object[] record = this.index.lookupByKey( entry.getKey().getPk() );
            Object[] projection = new Object[this.projectionColumns.length];
            j = 0;
            for (int projectionColumn : this.projectionColumns) {
                projection[j] = record[projectionColumn];
                j++;
            }
            result.add(projection);
            i++;
            if(i == this.limit) break;
        }
        return result;
    }

//    private void project(Map<GroupByKey, Comparable<?>> minMap){
//        int i = 0;
//        for(var entry : minMap.entrySet()){
//            this.append(entry.getKey());
//            i++;
//            if(i == this.limit) break;
//        }
//    }

//    private void append(GroupByKey key) {
//        Object[] record = this.index.lookupByKey( key.getPk() );
//        for (int projectionColumn : this.projectionColumns) {
//            DataTypeUtils.callWriteFunction(this.currentBuffer.nextOffset(), schema.columnDataType(projectionColumn), record[projectionColumn]);
//            this.currentBuffer.forwardOffset(schema.columnDataType(projectionColumn).value);
//        }
//    }

    private void compute(Object[] record, Map<GroupByKey, Comparable<?>> minMap) {

        // hash the group by columns
        IKey pk = KeyUtils.buildRecordKey(schema.getPrimaryKeyColumns(), record);
        IKey groupByKey = KeyUtils.buildRecordKey(this.indexColumns, record);
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
