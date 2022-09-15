package dk.ku.di.dms.vms.modb.index;

import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterType;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordBucketIterator;
import dk.ku.di.dms.vms.modb.storage.memory.DataTypeUtils;

import java.util.HashSet;

/**
 * Base interface for operators that perform read-only queries
 * @param <K>
 */
public interface ReadOnlyIndex<K> {

    IIndexKey key();

    Schema schema();

    // columns that form the key to each record entry
    int[] columns();

    HashSet<Integer> columnsHash();

    /** information used by the planner to decide for the appropriate operator */
    IndexTypeEnum getType();

    default RecordBucketIterator iterator(IKey key){
        throw new IllegalStateException("No iterator for a key supported by this index.");
    }

    default IRecordIterator iterator(){
        throw new IllegalStateException("No iterator supported by this index.");
    }

    boolean exists(K key);

    boolean exists(long address);

    /**
     * This method may behave differently across indexes
     * For non unique, must return the address of the bucket
     * For unique, the address of the record
     * @param key record key
     * @return the record address
     */
    long retrieve(K key);

    default UniqueHashIndex asUniqueHashIndex(){
        throw new IllegalStateException("Concrete index does not override this method.");
    }

    default NonUniqueHashIndex asNonUniqueHashIndex(){
        throw new IllegalStateException("Concrete index does not override this method.");
    }

    default boolean checkCondition(IRecordIterator iterator, FilterContext filterContext){
        return checkCondition( iterator.current(), filterContext );
    }

    default boolean checkCondition(K key, long address, FilterContext filterContext){
        return checkCondition( address, filterContext );
    }

    default long getColumnAddress(IRecordIterator iterator, int columnIndex){
        return iterator.current() + schema().getColumnOffset(columnIndex);
    }

    default long getColumnAddress(K key, long address, int columnIndex){
        return address + schema().getColumnOffset(columnIndex);
    }

    default IKey hashAggregateGroup(K key, long address, int[] indexColumns){
        return KeyUtils.buildRecordKey(schema(), indexColumns, address);
    }

    default IKey hashAggregateGroup(IRecordIterator iterator, int[] indexColumns){
        return KeyUtils.buildRecordKey(schema(), indexColumns, iterator.current());
    }

    /**
     * This is the basic check condition. Does not take into consideration the
     * versioned values.
     * @param address src address of the record
     * @param filterContext the filter to be applied
     * @return
     */
    @SuppressWarnings("unchecked")
    private boolean checkCondition(long address, FilterContext filterContext){

        if(!exists(address)) return false;
        if(filterContext == null) return true;

        boolean conditionHolds = true;

        // the number of filters to apply
        int filterIdx = 0;

        // the filter index on which a given param (e.g., literals, zero, 1, 'SURNAME', etc) should apply
        int biPredIdx = 0;

        // simple predicates, do not involve input params (i.e, NULL, NOT NULL, EXISTS?, etc)
        int predIdx = 0;

        while( conditionHolds && filterIdx < filterContext.filterTypes.size() ){

            // no need to read active bit

            int columnIndex = filterContext.filterColumns.get(filterIdx);
            int columnOffset = schema().getColumnOffset( columnIndex );
            DataType dataType = schema().getColumnDataType( columnIndex );

            // how to get the versioned value?
            Object val = DataTypeUtils.getValue( dataType, address + columnOffset );

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

}
