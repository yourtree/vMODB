package dk.ku.di.dms.vms.modb.transaction.multiversion.index;

import dk.ku.di.dms.vms.modb.common.constraint.ConstraintEnum;
import dk.ku.di.dms.vms.modb.common.constraint.ConstraintReference;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionMetadata;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterType;
import dk.ku.di.dms.vms.modb.transaction.multiversion.IPrimaryKeyGenerator;
import dk.ku.di.dms.vms.modb.transaction.multiversion.OperationSetOfKey;
import dk.ku.di.dms.vms.modb.transaction.multiversion.TransactionWrite;
import dk.ku.di.dms.vms.modb.transaction.multiversion.WriteType;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static dk.ku.di.dms.vms.modb.common.constraint.ConstraintConstants.*;

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
 * The append operation must take into account the correct data item version
 * Q: Why implement a ReadOnlyIndex?
 * A: Because it is not supposed to modify the data in main-memory,
 * but rather keep versions in a cache on heap memory.
 * Thus, the API for writes are different. The ReadWriteIndex is only used for bulk writes,
 * when no transactional guarantees are necessary.
 */
public final class PrimaryIndex implements IMultiVersionIndex {

    private final ReadWriteIndex<IKey> primaryKeyIndex;

    private final Map<IKey, OperationSetOfKey> updatesPerKeyMap;

    // for PK generation. for now, all strategies use this (auto, sequence, etc)
    private final IPrimaryKeyGenerator<?> primaryKeyGenerator;

    /**
     * Optimization is verifying whether this thread is R or RW.
     * If R, no need to allocate a List
     */
    private final ThreadLocal<List<IKey>> KEY_WRITES = ThreadLocal.withInitial(() -> {
        if(!TransactionMetadata.TRANSACTION_CONTEXT.get().readOnly) {
            var pulled = writeListBuffer.poll();
            if(pulled == null)
                return new ArrayList<>(2);
            return pulled;
        }
        // immutable, empty list for read-only transactions
        return Collections.emptyList();
    });

    private static final Deque<List<IKey>> writeListBuffer = new ArrayDeque<>();

    public PrimaryIndex(ReadWriteIndex<IKey> primaryKeyIndex) {
        this.primaryKeyIndex = primaryKeyIndex;
        this.updatesPerKeyMap = new ConcurrentHashMap<>();
        this.primaryKeyGenerator = null;
    }

    public PrimaryIndex(ReadWriteIndex<IKey> primaryKeyIndex, IPrimaryKeyGenerator<?> primaryKeyGenerator) {
        this.primaryKeyIndex = primaryKeyIndex;
        this.updatesPerKeyMap = new ConcurrentHashMap<>();
        this.primaryKeyGenerator = primaryKeyGenerator;
    }

    @Override
    public boolean equals(Object o) {
        return this.hashCode() == o.hashCode();
    }

    @Override
    public int hashCode() {
        return this.primaryKeyIndex.key().hashCode();
    }

    /**
     * Same logic as {@link #lookupByKey}
     * @param key record key
     * @return whether the record exists for this transaction, respecting atomic visibility
     */
    public boolean exists(IKey key) {

        // O(1)
        OperationSetOfKey opSet = this.updatesPerKeyMap.get(key);

        if(opSet != null){

            // why checking first if I am a WRITE. because by checking if I am right, I don't need to pay O(log n)
            // 1 write thread at a time. if that is a writer thread, does not matter my lastTid. I can just check the last write for this entry
            if( !TransactionMetadata.TRANSACTION_CONTEXT.get().readOnly ){
                return opSet.lastWriteType != WriteType.DELETE;
            }

            // O(log n)
            var floorEntry = opSet.updateHistoryMap.floorEntry(TransactionMetadata.TRANSACTION_CONTEXT.get().lastTid);
            if(floorEntry == null) return this.primaryKeyIndex.exists(key);
            return floorEntry.val().type != WriteType.DELETE;
        }

        return this.primaryKeyIndex.exists(key);

    }



//    @Override
//    public long address(IKey key) {
//        return this.primaryKeyIndex.address(key);
//    }


    // every condition checked is proceeded by a completion handler
    // can maintain the boolean return but all operators (for read queries)
    // necessarily require appending to a memory result space


    /**
     * This is the basic check condition. Does not take into consideration the
     * versioned values.
     * @param record record values
     * @param filterContext the filter to be applied
     * @return the correct data item version
     */
    @SuppressWarnings("unchecked")
    public boolean checkConditionVersioned(FilterContext filterContext, Object[] record){

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

    /**
     * Methods for insert/update operations.
     */
    private boolean nonPkConstraintViolation(Object[] values) {

        if(this.primaryKeyIndex.schema().constraints().isEmpty())
            return false;

        Schema schema = this.primaryKeyIndex.schema();
        boolean constraintHolds = true;

        for(Map.Entry<Integer, ConstraintReference> c : schema.constraints().entrySet()) {

            switch (c.getValue().constraint.type){

                case NUMBER -> {
                    switch (schema.columnDataType(c.getKey())) {
                        case INT -> constraintHolds = NumberTypeConstraintHelper.eval((int)values[c.getKey()] , 0, Integer::compareTo, c.getValue().constraint);
                        case LONG, DATE -> constraintHolds = NumberTypeConstraintHelper.eval((long)values[c.getKey()] , 0L, Long::compareTo, c.getValue().constraint);
                        case FLOAT -> constraintHolds = NumberTypeConstraintHelper.eval((float)values[c.getKey()] , 0f, Float::compareTo, c.getValue().constraint);
                        case DOUBLE -> constraintHolds = NumberTypeConstraintHelper.eval((double)values[c.getKey()] , 0d, Double::compareTo, c.getValue().constraint);
                        default -> throw new IllegalStateException("Data type "+c.getValue().constraint.type+" cannot be applied to a number");
                    }
                }

                case NUMBER_WITH_VALUE -> {
                    Object valToCompare = c.getValue().asValueConstraint().value;
                    switch (schema.columnDataType(c.getKey())) {
                        case INT -> constraintHolds = NumberTypeConstraintHelper.eval((int)values[c.getKey()] , (int)valToCompare, Integer::compareTo, c.getValue().constraint);
                        case LONG, DATE -> constraintHolds = NumberTypeConstraintHelper.eval((long)values[c.getKey()] , (long)valToCompare, Long::compareTo, c.getValue().constraint);
                        case FLOAT -> constraintHolds = NumberTypeConstraintHelper.eval((float)values[c.getKey()] , (float)valToCompare, Float::compareTo, c.getValue().constraint);
                        case DOUBLE -> constraintHolds = NumberTypeConstraintHelper.eval((double)values[c.getKey()] , (double)valToCompare, Double::compareTo, c.getValue().constraint);
                        default -> throw new IllegalStateException("Data type "+c.getValue().constraint.type+" cannot be applied to a number");
                    }
                }

                case NULLABLE ->
                        constraintHolds = NullableTypeConstraintHelper.eval(values[c.getKey()], c.getValue().constraint);

                case CHARACTER ->
                        constraintHolds = CharOrStringTypeConstraintHelper.eval((String) values[c.getKey()], c.getValue().constraint );

            }

            if(!constraintHolds) return true;

        }

        return false;

    }

    private static class NullableTypeConstraintHelper {
        public static <T> boolean eval(T v1, ConstraintEnum constraint){
            if (constraint == ConstraintEnum.NOT_NULL) {
                return v1 != null;
            }
            return v1 == null;
        }
    }

    private static class CharOrStringTypeConstraintHelper {

        public static boolean eval(String value, ConstraintEnum constraint){

            if (constraint == ConstraintEnum.NOT_BLANK) {
                for(int i = 0; i < value.length(); i++){
                    if(value.charAt(i) != ' ') return true;
                }
            } else {
                // TODO support pattern, non blank
                throw new IllegalStateException("Constraint cannot be applied to characters.");
            }
            return false;
        }

    }

    /**
     * Having the second parameter is necessary to avoid casting.
     */
    private static class NumberTypeConstraintHelper {

        public static <T> boolean eval(T v1, T v2, Comparator<T> comparator, ConstraintEnum constraint){

            switch (constraint) {

                case POSITIVE_OR_ZERO, MIN -> {
                    return comparator.compare(v1, v2) >= 0;
                }
                case POSITIVE -> {
                    return comparator.compare(v1, v2) > 0;
                }
                case NEGATIVE -> {
                    return comparator.compare(v1, v2) < 0;
                }
                case NEGATIVE_OR_ZERO, MAX -> {
                    return comparator.compare(v1, v2) <= 0;
                }
                default ->
                        throw new IllegalStateException("Cannot compare the constraint "+constraint+" for number type.");
            }

        }

    }

    @Override
    public Object[] lookupByKey(IKey key){
        OperationSetOfKey operationSet = this.updatesPerKeyMap.get( key );
        if ( operationSet != null ){
            if(TransactionMetadata.TRANSACTION_CONTEXT.get().readOnly) {
                var entry = operationSet.updateHistoryMap.floorEntry(TransactionMetadata.TRANSACTION_CONTEXT.get().lastTid);
                if (entry != null){
                    return (entry.val().type != WriteType.DELETE ? entry.val().record : null);
                }
            } else
                return operationSet.lastWriteType != WriteType.DELETE ? operationSet.lastVersion : null;
        }

        // it is a readonly
        if(this.primaryKeyIndex.exists(key)) {
            return this.primaryKeyIndex.lookupByKey(key);
        }

        return null;
    }

    /**
     * TODO if cached value is not null, then extract the updated columns to make constraint violation check faster
     * @return whether it is allowed to proceed with the operation
     */
    public boolean insert(IKey key, Object[] values) {
        OperationSetOfKey operationSet = this.updatesPerKeyMap.get( key );

        boolean pkConstraintViolation;
        // if not delete, violation (it means some tid has written to this PK before)
        if ( operationSet != null ){
            pkConstraintViolation = operationSet.lastWriteType != WriteType.DELETE;
        } else {
            // let's check now the index itself. it exists, it is a violation
            pkConstraintViolation = this.primaryKeyIndex.exists(key);
        }

        if(pkConstraintViolation || nonPkConstraintViolation(values)) {
            return false;
        }

        // create a new insert
        TransactionWrite entry = new TransactionWrite(WriteType.INSERT, values);

        if(operationSet == null){
            operationSet = new OperationSetOfKey();
            this.updatesPerKeyMap.put( key, operationSet );
        }

        operationSet.updateHistoryMap.put( TransactionMetadata.TRANSACTION_CONTEXT.get().tid, entry);
        operationSet.lastWriteType = WriteType.INSERT;
        operationSet.lastVersion = values;

        KEY_WRITES.get().add(key);

        return true;
    }

    @Override
    public boolean update(IKey key, Object[] values) {

        OperationSetOfKey operationSet = this.updatesPerKeyMap.get( key );

        boolean pkConstraintViolation;
        if ( operationSet != null ){
            pkConstraintViolation = operationSet.lastWriteType == WriteType.DELETE;
        } else {
            pkConstraintViolation = !primaryKeyIndex.exists(key);
        }

        if(pkConstraintViolation || nonPkConstraintViolation(values)) {
            return false;
        }

        // create a new update
        TransactionWrite entry = new TransactionWrite(WriteType.UPDATE, values);

        if(operationSet == null){
            operationSet = new OperationSetOfKey();
            this.updatesPerKeyMap.put( key, operationSet );
        }

        operationSet.updateHistoryMap.put( TransactionMetadata.TRANSACTION_CONTEXT.get().tid, entry);
        operationSet.lastWriteType = WriteType.UPDATE;
        operationSet.lastVersion = values;

        KEY_WRITES.get().add(key);

        return true;
    }

    public IKey insertAndGetKey(Object[] values){
        if(this.primaryKeyGenerator != null){
            Object key_ = this.primaryKeyGenerator.next();
            values[this.primaryKeyIndex.columns()[0]] = key_;
            IKey key = KeyUtils.buildRecordKey( this.primaryKeyIndex.schema().getPrimaryKeyColumns(),  key_ );
            if(this.insert( key, values )){
                return key;
            }
        } else {
            IKey key = KeyUtils.buildRecordKey(this.primaryKeyIndex.schema().getPrimaryKeyColumns(), values);
            if(this.insert( key, values )){
                return key;
            }
        }
        return null;
    }

    @Override
    public boolean remove(IKey key) {

        // O(1)
        OperationSetOfKey operationSet = this.updatesPerKeyMap.get( key );

        if ( operationSet != null ){

            if(operationSet.lastWriteType != WriteType.DELETE){

                TransactionWrite entry = new TransactionWrite(WriteType.DELETE, null);
                // amortized O(1)
                operationSet.updateHistoryMap.put(TransactionMetadata.TRANSACTION_CONTEXT.get().tid, entry);
                operationSet.lastWriteType = WriteType.DELETE;

                KEY_WRITES.get().add(key);

                return true;

            }
            // does this key even exist? if not, don't even need to save it on transaction metadata

        } else if(this.primaryKeyIndex.exists(key)) {

            // that means we haven't had any previous transaction performing writes to this key
            operationSet = new OperationSetOfKey();
            this.updatesPerKeyMap.put( key, operationSet );

            TransactionWrite entry = new TransactionWrite(WriteType.DELETE, null);

            operationSet.updateHistoryMap.put(TransactionMetadata.TRANSACTION_CONTEXT.get().tid, entry);
            operationSet.lastWriteType = WriteType.DELETE;

            KEY_WRITES.get().add(key);

            return true;

        }

        return false;

    }

    /**
     * Called when a constraint is violated, leading to a transaction abort
     */
    public void undoTransactionWrites(){

        var writesOfTid = KEY_WRITES.get();
        if(writesOfTid == null) return;

        for(var key : writesOfTid) {
            // do we have a record written in the corresponding index? always yes. if no, it is a bug
            OperationSetOfKey operationSetOfKey = this.updatesPerKeyMap.get(key);
            operationSetOfKey.updateHistoryMap.poll();
        }

        writesOfTid.clear();
        writeListBuffer.add(writesOfTid);
        KEY_WRITES.set(null);

    }

    public void installWrites(){

        for(Map.Entry<IKey, OperationSetOfKey> entry : this.updatesPerKeyMap.entrySet()) {

            OperationSetOfKey operationSetOfKey = this.updatesPerKeyMap.get(entry.getKey());

            switch (operationSetOfKey.lastWriteType){
                case UPDATE -> this.primaryKeyIndex.update(entry.getKey(), operationSetOfKey.lastVersion);
                case INSERT -> this.primaryKeyIndex.insert(entry.getKey(), operationSetOfKey.lastVersion);
                case DELETE -> this.primaryKeyIndex.delete(entry.getKey());
            }

            operationSetOfKey.updateHistoryMap.clear();

        }

    }

    public ReadWriteIndex<IKey> underlyingIndex(){
        return this.primaryKeyIndex;
    }

}
