package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterType;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordBucketIterator;
import dk.ku.di.dms.vms.modb.storage.memory.DataTypeUtils;
import dk.ku.di.dms.vms.modb.transaction.multiversion.OperationNode;
import dk.ku.di.dms.vms.modb.transaction.multiversion.operation.Operation;

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

    private AbstractIndex<IKey> index;

    /**
     * Since the previous checkpointed state, multiple updates may have been applied
     *
     */
    private Map<IKey, OperationNode> keyVersionMap;


    @Override
    public Schema schema() {
        return index.schema();
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
        return keyVersionMap.get(key).operation.operation() != Operation.DELETE;
        // then it is update or insert
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
        return checkConditionVersioned(pk, filterContext);
    }

    @Override
    public boolean checkCondition(IKey key, FilterContext filterContext) {
        if(!this.exists(key)) return false;
        return checkConditionVersioned(key, filterContext);
    }

    /**
     * This is the basic check condition. Does not take into consideration the
     * versioned values.
     * @param key record key
     * @param filterContext the filter to be applied
     * @return
     */
    @SuppressWarnings("unchecked")
    boolean checkConditionVersioned(IKey key, FilterContext filterContext){

        if(filterContext == null) return true;

        long srcAddress = index.retrieve(key);

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
        Object val;
        int columnOffset;
        OperationNode currNode = keyVersionMap.get(key);
        if (currNode == null) {
            columnOffset = schema().getColumnOffset(columnIndex);
            val = DataTypeUtils.getValue(dataType, srcAddress + columnOffset );
        } else {
            if(keyVersionMap.get(key).operation.operation() == Operation.INSERT){
                columnOffset = schema().getColumnOffset(columnIndex);
                val = DataTypeUtils.getValue(dataType, currNode.operation.asInsert().bufferAddress + columnOffset );
            } else {

                // find the column, if possible
                while(currNode.operation.asUpdate().columnIndex < columnIndex){
                    currNode = currNode.next;
                    if(currNode == null) break;
                }

                if(currNode != null && currNode.operation.asUpdate().columnIndex == columnIndex){
                    val = DataTypeUtils.getValue(dataType, currNode.operation.asUpdate().address );
                } else {
                    columnOffset = schema().getColumnOffset(columnIndex);
                    val = DataTypeUtils.getValue(dataType, srcAddress + columnOffset );
                }

            }

        }
        return val;
    }

}
