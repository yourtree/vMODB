package dk.ku.di.dms.vms.modb.transaction.multiversion.index;

import dk.ku.di.dms.vms.modb.common.constraint.ConstraintEnum;
import dk.ku.di.dms.vms.modb.common.constraint.ConstraintReference;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionId;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionMetadata;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.transaction.internal.SingleWriterMultipleReadersFIFO;
import dk.ku.di.dms.vms.modb.transaction.multiversion.IPrimaryKeyGenerator;
import dk.ku.di.dms.vms.modb.transaction.multiversion.OperationSetOfKey;
import dk.ku.di.dms.vms.modb.transaction.multiversion.TransactionWrite;
import dk.ku.di.dms.vms.modb.transaction.multiversion.WriteType;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

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
    private final Optional<IPrimaryKeyGenerator<?>> primaryKeyGenerator;

    /**
     * Optimization is verifying whether this thread is R or RW.
     * If R, no need to allocate a List
     */
    private final ThreadLocal<Set<IKey>> KEY_WRITES = ThreadLocal.withInitial(() -> {
        if(!TransactionMetadata.TRANSACTION_CONTEXT.get().readOnly) {
            Set<IKey> pulled = WRITE_SET_BUFFER.poll();
            if(pulled == null)
                return new HashSet<>();
            return pulled;
        }
        // immutable, empty list for read-only transactions
        return Collections.emptySet();
    });

    private static final Deque<Set<IKey>> WRITE_SET_BUFFER = new ConcurrentLinkedDeque<>();

    public PrimaryIndex(ReadWriteIndex<IKey> primaryKeyIndex) {
        this.primaryKeyIndex = primaryKeyIndex;
        this.updatesPerKeyMap = new ConcurrentHashMap<>();
        this.primaryKeyGenerator = Optional.empty();
    }

    public PrimaryIndex(ReadWriteIndex<IKey> primaryKeyIndex, IPrimaryKeyGenerator<?> primaryKeyGenerator) {
        this.primaryKeyIndex = primaryKeyIndex;
        this.updatesPerKeyMap = new ConcurrentHashMap<>();
        this.primaryKeyGenerator = Optional.of( primaryKeyGenerator );
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


    // every condition checked is proceeded by a completion handler
    // can maintain the boolean return but all operators (for read queries)
    // necessarily require appending to a memory result space

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

        operationSet.updateHistoryMap.put(TransactionMetadata.TRANSACTION_CONTEXT.get().tid, entry);
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
        if(this.primaryKeyGenerator.isPresent()){
            Object key_ = this.primaryKeyGenerator.get().next();
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

    @Override
    public int[] indexColumns() {
        return this.primaryKeyIndex.columns();
    }

    @Override
    public boolean containsColumn(int columnPos) {
        return this.primaryKeyIndex.containsColumn(columnPos);
    }

    /**
     * Called when a constraint is violated, leading to a transaction abort
     */
    public void undoTransactionWrites(){
        Set<IKey> writesOfTid = KEY_WRITES.get();
        if(writesOfTid == null) return;

        for(var key : writesOfTid) {
            // do we have a record written in the corresponding index? always yes. if no, it is a bug
            OperationSetOfKey operationSetOfKey = this.updatesPerKeyMap.get(key);
            operationSetOfKey.updateHistoryMap.poll();
        }

        writesOfTid.clear();
        // TODO perhaps don't need to return it, since the same thread will be running logic again....
        WRITE_SET_BUFFER.add(writesOfTid);
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

    /**
     * Mostly applies to full scan
     */
    @Override
    public Iterator<Object[]> iterator() {
        // TODO integrate with underlying iterator
        // 1 - collect all records eligible in this.updatesPerKeyMap (use a hashmap)
        // 2 - build a iterator object that iterates over the records of the underling index
        // 3. for each key, check if it is present in the set (1)
        // 3a. if so, just return the fresh record as next. remember to remove it from the set
        // 3b. otherwise, return the object as it is
        // 4. when the iteration over the underlying index finishes, continue from the remaining set
        return new MultiVersionIterator();
    }

    protected class MultiVersionIterator implements Iterator<Object[]>{

        // Map<IKey, Object[]> freshSet;
        Iterator<Map.Entry<IKey,Object[]>> freshSetIterator;

        public MultiVersionIterator(){
            this.freshSetIterator = collectFreshSet().entrySet().iterator();
        }

        public MultiVersionIterator(IKey[] keys){
            this.freshSetIterator = collectFreshSet(keys).entrySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return this.freshSetIterator.hasNext();
        }

        @Override
        public Object[] next() {
            return this.freshSetIterator.next().getValue();
        }
    }

    private Map<IKey, Object[]> collectFreshSet() {
        Map<IKey, Object[]> freshSet = new HashMap<>();
        // iterate over keys
        for(Map.Entry<IKey, OperationSetOfKey> entry : this.updatesPerKeyMap.entrySet()) {
            var obj = entry.getValue().updateHistoryMap.floorEntry(TransactionMetadata.TRANSACTION_CONTEXT.get().lastTid);
            if (obj != null) {
                freshSet.put(entry.getKey(), obj.val().record);
            }
        }
        return freshSet;
    }

    private Map<IKey, Object[]> collectFreshSet(IKey[] keys) {
        Map<IKey, Object[]> freshSet = new HashMap<>();
        // iterate over keys
        for(IKey key : keys){
            var operation = this.updatesPerKeyMap.get(key);
            SingleWriterMultipleReadersFIFO.Entry<TransactionId, TransactionWrite> obj = operation.updateHistoryMap.floorEntry(TransactionMetadata.TRANSACTION_CONTEXT.get().lastTid);
            if (obj != null) {
                freshSet.put(key, obj.val().record);
            }
        }
        return freshSet;
    }

    public SingleWriterMultipleReadersFIFO.Entry<TransactionId, TransactionWrite> getFloorEntry(IKey key){
        OperationSetOfKey operation = this.updatesPerKeyMap.get(key);
        return operation.updateHistoryMap.floorEntry(TransactionMetadata.TRANSACTION_CONTEXT.get().lastTid);
    }

    @Override
    public Iterator<Object[]> iterator(IKey[] keys) {
        return new MultiVersionIterator(keys);
    }

}
