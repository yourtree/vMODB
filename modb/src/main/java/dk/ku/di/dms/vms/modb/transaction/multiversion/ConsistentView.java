package dk.ku.di.dms.vms.modb.transaction.multiversion;

import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionId;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Header;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.SimpleKey;
import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterType;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.modb.transaction.TransactionFacade;
import jdk.internal.misc.Unsafe;

import java.util.HashSet;
import java.util.Map;

/**
 * A consistent view over an index.
 * A wrapper that envelops the original index
 * <a href="https://refactoring.guru/design-patterns">...</a>
 * To implement a consistent view correctly,
 * we need correct access to versioned data items,
 * correct copying of data items to the result buffers.
 * So it is a mix of behavioral and state:
 * The operators need an iterator (to provide only the allowed data items' visibility)
 * The filter must take into account the correct data item version
 * The append must take into account the correct data item version
 */
public class ConsistentView implements ReadOnlyIndex<IKey> {

    private static final Unsafe UNSAFE = MemoryUtils.UNSAFE;

    private final ReadOnlyIndex<IKey> index;

    /**
     * Since the previous checkpointed state, multiple updates may have been applied
     *
     */
    private final Map<IKey, OperationSetOfKey> keyOperationSetAcrossTIDs;

    private final Map<IKey, DataItemVersion> writesOfThisTransaction;

    /**
     * To allow for atomic visibility among tasks of the same TID
     */
    private final TransactionId lastTransactionId;

    public ConsistentView(ReadOnlyIndex<IKey> index,
                          Map<IKey, DataItemVersion> writesOfThisTransaction,
                          Map<IKey, OperationSetOfKey> keyOperationSetAcrossTIDs,
                          TransactionId lastTransactionId) {
        this.index = index;
        this.writesOfThisTransaction = writesOfThisTransaction;
        this.keyOperationSetAcrossTIDs = keyOperationSetAcrossTIDs;
        this.lastTransactionId = lastTransactionId;
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
    public IRecordIterator iterator(IKey key) {
        return new ConsistentRecordIterator(this, index.iterator(key));
    }

    @Override
    public IRecordIterator iterator() {
        return new ConsistentRecordIterator(this, index.iterator());
    }

    /**
     * Same logic as {@link TransactionFacade#lookupByKey(Table, Object...)}
     * @param key record key
     * @return whether the record exists for this transaction, respecting atomic visibility
     */
    @Override
    public boolean exists(IKey key) {

        // O(1)
        if(writesOfThisTransaction != null &&
                writesOfThisTransaction.get(key) != null &&
                writesOfThisTransaction.get(key).entry.type != WriteType.DELETE)
            return true;

        // O(1)
        if(keyOperationSetAcrossTIDs != null && keyOperationSetAcrossTIDs.get(key) != null){
            OperationSetOfKey opSet = keyOperationSetAcrossTIDs.get(key);

            // O(log n)
            var floorEntry = opSet.updateHistoryMap.floorEntry( lastTransactionId );
            if(floorEntry == null) return index.exists(key);

            return floorEntry.getValue().type != WriteType.DELETE;
        }

        return index.exists(key);

    }

    private Object[] existsReturningTheObjectIfExists(IKey key, long address) {

        // O(1)
        if(writesOfThisTransaction != null &&
                writesOfThisTransaction.get(key) != null &&
                writesOfThisTransaction.get(key).entry.type != WriteType.DELETE)
            return writesOfThisTransaction.get(key).entry.record;

        // O(1)
        if(keyOperationSetAcrossTIDs != null && keyOperationSetAcrossTIDs.get(key) != null) {
            OperationSetOfKey opSet = keyOperationSetAcrossTIDs.get(key);

            // O(log n)
            var floorEntry = opSet.updateHistoryMap.floorEntry( lastTransactionId );
            boolean floorIsNull = floorEntry == null;
            if(floorIsNull && index.exists( address )) return index.readFromIndex(address);

            return !floorIsNull && floorEntry.getValue().type != WriteType.DELETE ? floorEntry.getValue().record : null;
        }

        return index.readFromIndex(address);

    }

    private Object[] existsReturningTheObjectIfExists(IKey key) {

        // O(1)
        if(writesOfThisTransaction != null &&
                writesOfThisTransaction.get(key) != null &&
                writesOfThisTransaction.get(key).entry.type != WriteType.DELETE)
            return writesOfThisTransaction.get(key).entry.record;

        // O(1)
        if(keyOperationSetAcrossTIDs != null && keyOperationSetAcrossTIDs.get(key) != null) {
            OperationSetOfKey opSet = keyOperationSetAcrossTIDs.get(key);

            // O(log n)
            var floorEntry = opSet.updateHistoryMap.floorEntry( lastTransactionId );
            boolean floorIsNull = floorEntry == null;
            if(floorIsNull && index.exists( key )) return index.readFromIndex(key);

            return !floorIsNull && floorEntry.getValue().type != WriteType.DELETE ? floorEntry.getValue().record : null;
        }

        return index.readFromIndex(key);

    }

    @Override
    public Object[] readFromIndex(IKey key, long address) {
        return existsReturningTheObjectIfExists(key, address);
    }

    @Override
    public  Object[] readFromIndex(IKey key) {
        return existsReturningTheObjectIfExists(key);
    }

    @Override
    public boolean exists(long address) {
        if(index.exists(address)) {
            // existing before does not mean it has not been been deleted
            int pk = UNSAFE.getInt( address + Header.SIZE );
            var key = SimpleKey.of(pk);
            return this.exists( key );
        }
        return false;
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
        IKey key = iterator.primaryKey();
        Object[] record = existsReturningTheObjectIfExists(key);
        if(record == null) return false;
        return checkConditionVersioned(record, filterContext);
    }

    @Override
    public boolean checkCondition(IKey key, long address, FilterContext filterContext) {
        Object[] record = existsReturningTheObjectIfExists(key, address);
        if(record == null) return false;
        return checkConditionVersioned(record, filterContext);
    }

    /**
     * This is the basic check condition. Does not take into consideration the
     * versioned values.
     * @param record record values
     * @param filterContext the filter to be applied
     * @return the correct data item version
     */
    @SuppressWarnings("unchecked")
    boolean checkConditionVersioned(Object[] record, FilterContext filterContext){

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

            Object val = record[columnIndex];

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
