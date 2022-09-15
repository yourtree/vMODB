package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterType;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordBucketIterator;
import dk.ku.di.dms.vms.modb.storage.memory.DataTypeUtils;
import dk.ku.di.dms.vms.modb.transaction.multiversion.OperationSet;
import dk.ku.di.dms.vms.modb.transaction.multiversion.operation.UpdateOp;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * A consistent view over an index
 *
 * https://refactoring.guru/design-patterns
 *
 * To implement a consistent view correctly,
 * we need correct access to versioned data items,
 * correct copying of data items to the result buffers
 *
 * So it is a mix of behavioral and state:
 *
 * The operators need an iterator (to provide only the allowed data items' visibility)
 * The filter must take into account the correct data item version
 * The append must take into account the correct data item version
 *
 */
public class ConsistentView implements ReadOnlyIndex<IKey> {

    private ReadOnlyIndex<IKey> index;

    /**
     * Since the previous checkpointed state, multiple updates may have been applied
     *
     */
    private Map<IKey, OperationSet> keyVersionMap;

    private long tid;

    public ConsistentView(ReadOnlyIndex<IKey> index, Map<IKey, OperationSet> keyVersionMap, long tid) {
        this.index = index;
        this.keyVersionMap = keyVersionMap;
        this.tid = tid;
    }

    @Override
    public IIndexKey key() {
        return index.key();
    }

    @Override
    public Schema schema() {
        return index.schema();
    }

    @Override
    public int[] columns() {
        return index.columns();
    }

    @Override
    public HashSet<Integer> columnsHash() {
        return index.columnsHash();
    }

    @Override
    public IndexTypeEnum getType() {
        return index.getType();
    }

    @Override
    public RecordBucketIterator iterator(IKey key) {
        //return ReadOnlyIndex.super.iterator(key);
        return null;
    }

    @Override
    public IRecordIterator iterator() {
        //return ReadOnlyIndex.super.iterator();
        return null;
    }

    @Override
    public boolean exists(IKey key) {
        if(keyVersionMap.get(key) == null) return index.exists(key);
        // delete always come first, if not delete, then it is insert.
        // if there is no delete and insert, then set of updates ordered by column index
        OperationSet opSet = keyVersionMap.get(key);
        // then it has been previously deleted (even by this same transaction)
        return opSet.deleteOp != null && opSet.deleteOp.tid() <= this.tid;
        // then it is update or insert
    }

    @Override
    public boolean exists(long address) {
        return index.exists(address);
    }

    @Override
    public long retrieve(IKey key) {
        return index.retrieve(key);
    }

    @Override
    public UniqueHashIndex asUniqueHashIndex() {
        return index.asUniqueHashIndex();
    }

    @Override
    public NonUniqueHashIndex asNonUniqueHashIndex() {
        return index.asNonUniqueHashIndex();
    }

    // every condition checked is proceeded by a completion handler
    // can maintain the boolean return but all operators (for read queries)
    // necessarily require appending to a memory result space

    @Override
    public boolean checkCondition(IRecordIterator iterator, FilterContext filterContext) {
        IKey pk = iterator.primaryKey();
        if(!this.exists(pk)) return false;
        return checkConditionVersioned(pk, iterator.current(), filterContext);
    }

    @Override
    public boolean checkCondition(IKey key, long address, FilterContext filterContext) {
        if(!this.exists(key)) return false;
        return checkConditionVersioned(key, address, filterContext);
    }

    @Override
    public long getColumnAddress(IRecordIterator iterator, int columnIndex){
        return this.getColumnAddress(iterator.primaryKey(), iterator.current(), columnIndex);
    }

    /**
     * The columns iterated can be cached since many keys may require this same method.
     */
    @Override
    public long getColumnAddress(IKey key, long address, int columnIndex){

        OperationSet operationSet = keyVersionMap.get(key);
        if (operationSet == null) {
            return address + schema().getColumnOffset(columnIndex);
        }

        if(operationSet.insertOp != null && operationSet.insertOp.tid() <= tid){

            // besides, do we have a column update?
            if(operationSet.updateOps == null){
                int columnOffset = schema().getColumnOffset(columnIndex);
                return operationSet.insertOp.bufferAddress + columnOffset;
            } else {
                return getUpdatedColumnIfPossible(columnIndex, operationSet);
            }

        } else {
            return getUpdatedColumnIfPossible(columnIndex, operationSet);
        }

    }

    private long getUpdatedColumnIfPossible(int columnIndex, OperationSet operationSet) {
        // do we have a column update on this column index?
        List<UpdateOp> updateOpList = operationSet.updateOps.get(columnIndex);

        if(updateOpList == null || updateOpList.size() == 0){
            int columnOffset = schema().getColumnOffset(columnIndex);
            return operationSet.insertOp.bufferAddress + columnOffset;
        }

        // try to find the latest tid
        int index = updateOpList.size() - 1;
        while(index > 0 && updateOpList.get(index).tid() > tid){
            index--;
        }

        if(updateOpList.get(index).tid() <= tid){
            return updateOpList.get(index).address;
        }

        // in case even the last checked version does not apply to the current tid
        int columnOffset = schema().getColumnOffset(columnIndex);
        return operationSet.insertOp.bufferAddress + columnOffset;
    }

    /**
     * This is the basic check condition. Does not take into consideration the
     * versioned values.
     * @param key record key
     * @param filterContext the filter to be applied
     * @return the correct data item version
     */
    @SuppressWarnings("unchecked")
    boolean checkConditionVersioned(IKey key, long srcAddress, FilterContext filterContext){

        if(filterContext == null) return true;

        boolean conditionHolds = true;

        // the number of filters to apply
        int filterIdx = 0;

        // the filter index on which a given param (e.g., literals, zero, 1, 'SURNAME', etc) should apply
        int biPredIdx = 0;

        // simple predicates, do not involve input params (i.e, NULL, NOT NULL, EXISTS?, etc)
        int predIdx = 0;

        while( conditionHolds && filterIdx < filterContext.filterTypes.size() ){

            int columnIndex = filterContext.filterColumns.get(filterIdx);
            DataType dataType = schema().getColumnDataType( columnIndex );

            Object val = getDataItemColumnValue(key, srcAddress, columnIndex, dataType);

            // it is a literal passed to the query
            if(filterContext.filterTypes.get(filterIdx) == FilterType.BP) {
                conditionHolds = filterContext.biPredicates.get(biPredIdx).
                        apply(val, filterContext.biPredicateParams.get(biPredIdx));
                biPredIdx++;
            } else {
                conditionHolds = filterContext.predicates.get(predIdx).test( val );
                predIdx++;
            }

            filterIdx++;

        }

        return conditionHolds;

    }

    private Object getDataItemColumnValue(IKey key, long srcAddress, int columnIndex, DataType dataType) {
        long address = getColumnAddress(key, srcAddress, columnIndex);
        return DataTypeUtils.getValue(dataType, address );
    }

}
