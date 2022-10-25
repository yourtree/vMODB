package dk.ku.di.dms.vms.modb.index.interfaces;

import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterType;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

import java.util.List;

/**
 * Base interface for operators that perform read-only queries.
 * @param <K> The key object identifier of a record
 */
public interface ReadOnlyIndex<K> {

    IIndexKey key();

    Schema schema();

    // columns that form the key to each record entry
    int[] columns();

    boolean containsColumn(int columnPos);

    /** information used by the planner to decide for the appropriate operator */
    IndexTypeEnum getType();

    int size();

    boolean exists(K key);

    default boolean exists(long address){
        throw new IllegalStateException("No support for direct addressing in this index.");
    }

    /**
     * The address does not necessarily means the record exists in the database.
     * It is just the address that record would be copied into.
     * This method may behave differently across indexes
     * For non unique, must return the address of the bucket
     * For unique, the address of the record
     */
    default long address(K key) {
        throw new IllegalStateException("No support for direct addressing in this index.");
    }

    IRecordIterator<IKey> iterator();

    /**
     * For hash probing on a set of keys
     * Can also be implemented by non-unique hash indexes,
     * but may be expensive
     */
    IRecordIterator<IKey> iterator(IKey[] keys);

    default Object[] record(K key) {
        long address = this.address(key);
        return this.readFromIndex(address);
    }

    default Object[] record(IRecordIterator<IKey> iterator) {
        return this.readFromIndex(iterator.address());
    }

    default Object[] readFromIndex(long address) {
        int size = schema().columnOffset().length;
        Object[] objects = new Object[size];
        long currAddress = address;

        for(int i = 0; i < size; i++) {
            DataType dt = schema().columnDataType(i);
            objects[i] = DataTypeUtils.getValue(
                    dt,
                    currAddress );

            currAddress += dt.value;
        }
        return objects;
    }

    /**
     * Default call from operators
     * Multiversion-based iterators must override this method
     */
    default boolean checkCondition(IRecordIterator<K> iterator, FilterContext filterContext){
        return checkCondition( iterator.address(), filterContext );
    }

    /**
     * This is the basic check condition. Does not take into consideration the
     * versioned values.
     * @param address src address of the record
     * @param filterContext the filter to be applied
     * @return whether a record exists
     */
    @SuppressWarnings("unchecked")
    default boolean checkCondition(long address, FilterContext filterContext){

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
            int columnOffset = schema().columnOffset( columnIndex );
            DataType dataType = schema().columnDataType( columnIndex );

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
