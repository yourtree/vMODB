package dk.ku.di.dms.vms.modb.transaction.multiversion.index;

import dk.ku.di.dms.vms.modb.common.constraint.ConstraintEnum;
import dk.ku.di.dms.vms.modb.common.constraint.ConstraintReference;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.transaction.TransactionContext;
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

    private static final Deque<Set<IKey>> WRITE_SET_BUFFER = new ConcurrentLinkedDeque<>();

    private final ReadWriteIndex<IKey> primaryKeyIndex;

    private final Map<IKey, OperationSetOfKey> updatesPerKeyMap;

    // for PK generation. for now, all strategies use sequence
    private final Optional<IPrimaryKeyGenerator<?>> primaryKeyGenerator;

    private final Set<IKey> keysToFlush = ConcurrentHashMap.newKeySet();

    // write set of transactions
    private final Map<Long, Set<IKey>> writeSet;

    public static PrimaryIndex build(ReadWriteIndex<IKey> primaryKeyIndex) {
        return new PrimaryIndex(primaryKeyIndex, null);
    }

    public static PrimaryIndex build(ReadWriteIndex<IKey> primaryKeyIndex, IPrimaryKeyGenerator<?> primaryKeyGenerator){
        return new PrimaryIndex(primaryKeyIndex, primaryKeyGenerator);
    }

    private PrimaryIndex(ReadWriteIndex<IKey> primaryKeyIndex, IPrimaryKeyGenerator<?> primaryKeyGenerator) {
        this.primaryKeyIndex = primaryKeyIndex;
        this.updatesPerKeyMap = new ConcurrentHashMap<>(100000);
        this.primaryKeyGenerator = Optional.ofNullable(primaryKeyGenerator);
        this.writeSet = new ConcurrentHashMap<>();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PrimaryIndex && this.hashCode() == o.hashCode();
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
    public boolean exists(TransactionContext txCtx, IKey key) {
        OperationSetOfKey opSet = this.updatesPerKeyMap.get(key);
        if(opSet != null){
            // why checking first if I am a WRITE. because by checking if I am right, I don't need to pay O(log n)
            // 1 write thread at a time. if that is a writer thread, does not matter my lastTid. I can just check the last write for this entry
            if( !txCtx.readOnly ){
                return opSet.lastWriteType != WriteType.DELETE;
            }
            var floorEntry = opSet.updateHistoryMap.floorEntry(txCtx.lastTid);
            if(floorEntry == null) return this.primaryKeyIndex.exists(key);
            return floorEntry.val().type != WriteType.DELETE;
        }
        return this.primaryKeyIndex.exists(key);
    }

    public boolean meetPartialIndex(Object[] values, int column, Object expectedValue){
        Schema schema = this.primaryKeyIndex.schema();
        switch (schema.columnDataType(column)) {
            case CHAR -> {
                return ((String) values[column]).contentEquals((String) expectedValue);
            }
            case ENUM -> {
                return values[column].toString().contentEquals((String) expectedValue);
            }
        }
        return false;
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
    public Object[] lookupByKey(TransactionContext txCtx, IKey key){
        OperationSetOfKey operationSet = this.updatesPerKeyMap.get( key );
        if ( operationSet != null ){
            if(txCtx.readOnly) {
                var entry = operationSet.updateHistoryMap.floorEntry(txCtx.lastTid);
                if (entry != null){
                    return (entry.val().type != WriteType.DELETE ? entry.val().record : null);
                }
            } else {
                return operationSet.lastWriteType != WriteType.DELETE ?
                        operationSet.updateHistoryMap.peak().val().record : null;
            }
        }
        return this.primaryKeyIndex.lookupByKey(key);
    }

    /**
     * possible optimization: if cached value is not null,
     * then extract the updated columns to make constraint violation check faster
     * @return whether it is allowed to proceed with the operation
     */
    public boolean insert(TransactionContext txCtx, IKey key, Object[] values) {
        OperationSetOfKey operationSet = this.updatesPerKeyMap.get( key );
        boolean pkConstraintViolation;
        // if not delete, violation (it means some tid has written to this PK before)
        if ( operationSet != null ){
            pkConstraintViolation = operationSet.lastWriteType != WriteType.DELETE;
        } else {
            // let's check now the index itself. if the key exists, it is a violation
            pkConstraintViolation = this.primaryKeyIndex.exists(key);
        }
        if(pkConstraintViolation || this.nonPkConstraintViolation(values)) {
            return false;
        }
        return this.doInsert(txCtx, key, values, operationSet);
    }

    private boolean doInsert(TransactionContext txCtx, IKey key, Object[] values, OperationSetOfKey operationSet) {
        TransactionWrite entry = TransactionWrite.upsert(WriteType.INSERT, values);
        if(operationSet == null){
            operationSet = new OperationSetOfKey();
            this.updatesPerKeyMap.put(key, operationSet);
        }
        operationSet.updateHistoryMap.put(txCtx.tid, entry);
        operationSet.lastWriteType = WriteType.INSERT;
        this.appendWrite(txCtx, key);
        return true;
    }

    public boolean upsert(TransactionContext txCtx, IKey key, Object[] values) {
        if(this.nonPkConstraintViolation(values)) {
            return false;
        }
        OperationSetOfKey operationSet = this.updatesPerKeyMap.get( key );
        boolean exists;
        if (operationSet != null){
            exists = operationSet.lastWriteType != WriteType.DELETE;
        } else {
            exists = this.primaryKeyIndex.exists(key);
        }
        if(exists) {
            return this.doUpdate(txCtx, key, values, operationSet);
        } else {
            return this.doInsert(txCtx, key, values, operationSet);
        }
    }

    @Override
    public boolean update(TransactionContext txCtx, IKey key, Object[] values) {
        OperationSetOfKey operationSet = this.updatesPerKeyMap.get( key );
        boolean pkConstraintViolation;
        if (operationSet != null){
            pkConstraintViolation = operationSet.lastWriteType == WriteType.DELETE;
        } else {
            pkConstraintViolation = !this.primaryKeyIndex.exists(key);
        }
        if(pkConstraintViolation || this.nonPkConstraintViolation(values)) {
            return false;
        }
        return this.doUpdate(txCtx, key, values, operationSet);
    }

    private boolean doUpdate(TransactionContext txCtx, IKey key, Object[] values, OperationSetOfKey operationSet) {
        TransactionWrite entry = TransactionWrite.upsert(WriteType.UPDATE, values);
        if(operationSet == null){
            operationSet = new OperationSetOfKey();
            this.updatesPerKeyMap.put(key, operationSet);
        }
        operationSet.updateHistoryMap.put(txCtx.tid, entry);
        operationSet.lastWriteType = WriteType.UPDATE;
        this.appendWrite(txCtx, key);
        return true;
    }

    public IKey insertAndGetKey(TransactionContext txCtx, Object[] values){
        if(this.primaryKeyGenerator.isPresent()){
            Object key_ = this.primaryKeyGenerator.get().next();
            // set to record
            values[this.primaryKeyIndex.columns()[0]] = key_;
            IKey key = KeyUtils.buildRecordKey( this.primaryKeyIndex.schema().getPrimaryKeyColumns(), new Object[]{ key_ } );
            if(this.insert( txCtx, key, values )){
                return key;
            }
        } else {
            IKey key = KeyUtils.buildRecordKey(this.primaryKeyIndex.schema().getPrimaryKeyColumns(), values);
            if(this.insert( txCtx, key, values )){
                return key;
            }
        }
        return null;
    }

    @Override
    public boolean remove(TransactionContext txCtx, IKey key){
        return this.removeOpt(txCtx, key).isPresent();
    }

    public Optional<Object[]> removeOpt(TransactionContext txCtx, IKey key) {
        OperationSetOfKey operationSet = this.updatesPerKeyMap.get( key );
        if (operationSet != null && operationSet.lastWriteType != WriteType.DELETE){
            var entrySet = operationSet.updateHistoryMap.peak();
            Object[] lastRecord = entrySet.val().record;
            var lastWriteType = entrySet.val().type;
            TransactionWrite entry = TransactionWrite.delete(WriteType.DELETE);
            operationSet.updateHistoryMap.put(txCtx.tid, entry);
            operationSet.lastWriteType = WriteType.DELETE;
            this.appendWrite(txCtx, key);
            return Optional.of( lastRecord );
            // does this key even exist? if not, don't even need to save it on transaction metadata
        }
        Object[] obj = this.primaryKeyIndex.lookupByKey(key);
        if(obj != null) {
            // that means we haven't had any previous transaction performing writes to this key
            operationSet = new OperationSetOfKey();
            this.updatesPerKeyMap.put( key, operationSet );
            TransactionWrite entry = TransactionWrite.delete(WriteType.DELETE);
            operationSet.updateHistoryMap.put(txCtx.tid, entry);
            operationSet.lastWriteType = WriteType.DELETE;
            this.appendWrite(txCtx, key);
            return Optional.of( obj );
        }
        return Optional.empty();
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
    public void undoTransactionWrites(TransactionContext txCtx){
        var writeSet = this.removeWriteSet(txCtx);
        if(writeSet == null) return;
        for(var key : writeSet) {
            // do we have a record written in the corresponding index? always yes. if no, it is a bug
            OperationSetOfKey operationSetOfKey = this.updatesPerKeyMap.get(key);
            operationSetOfKey.updateHistoryMap.poll();
        }
        writeSet.clear();
        WRITE_SET_BUFFER.addLast(writeSet);
    }

    public void garbageCollection(long maxTid){
        for(var key : this.keysToFlush){
            OperationSetOfKey operationSetOfKey = this.updatesPerKeyMap.get(key);
            if(operationSetOfKey == null){
                throw new RuntimeException("Error on retrieving operation set for key "+key);
            }
            var entry = operationSetOfKey.updateHistoryMap.removeUpToEntry(maxTid);
            if(entry != null){
                // only remove from keys to flush if max tid meets the entry
                this.keysToFlush.remove(key);
            }
            /* for troubleshooting
            if(entry == null){
                var lastVersionEntry = operationSetOfKey.updateHistoryMap.peak();
                var next = lastVersionEntry.next();
                long nextVersionTID = 0;
                if(next != null){
                    nextVersionTID = next.key();
                }
                var lastVersion = lastVersionEntry.val().record;
                String lastVersionStr = lastVersion != null ?
                        Arrays.toString(lastVersion) : "null";
                System.out.println("It was not possible to find an entry for operation set key "+key+ " and maxTid "+maxTid+
                        "\nLast write TID: "+lastVersionEntry.key()+
                        "\nLast write type: "+operationSetOfKey.lastWriteType+
                        "\nLast version: "+lastVersionStr+
                        "\nNext version TID: "+nextVersionTID
                );
            }
            */
        }
    }

    public void checkpoint(long maxTid){
        for(var key : this.keysToFlush){
            OperationSetOfKey operationSetOfKey = this.updatesPerKeyMap.get(key);
            if(operationSetOfKey == null){
                throw new RuntimeException("Error on retrieving operation set for key "+key);
            }
            var entry = operationSetOfKey.updateHistoryMap.removeUpToEntry(maxTid);
            if (entry == null) continue;
            this.keysToFlush.remove(key);
            switch (operationSetOfKey.lastWriteType) {
                case UPDATE -> this.primaryKeyIndex.update(key, entry.val().record);
                case INSERT -> this.primaryKeyIndex.insert(key, entry.val().record);
                case DELETE -> this.primaryKeyIndex.delete(key);
            }
        }
        this.primaryKeyIndex.flush();
    }

    public void installWrites(TransactionContext txCtx){
        Set<IKey> writeSet = this.removeWriteSet(txCtx);
        this.keysToFlush.addAll(writeSet);
        writeSet.clear();
        WRITE_SET_BUFFER.addLast(writeSet);
    }

    public void appendWrite(TransactionContext txCtx, IKey key){
        this.writeSet.computeIfAbsent(txCtx.tid, k ->
                Objects.requireNonNullElseGet(WRITE_SET_BUFFER.poll(), HashSet::new)).add(key);
    }

    public Set<IKey> removeWriteSet(TransactionContext txCtx){
        return this.writeSet.remove(txCtx.tid);
    }

    public ReadWriteIndex<IKey> underlyingIndex(){
        return this.primaryKeyIndex;
    }

    @Override
    public Iterator<Object[]> iterator(TransactionContext txCtx, IKey... keys) {
        return new MultiVersionIterator(txCtx, keys);
    }

    /**
     * Used for scanning short number of keys
     * 1 - collect all records eligible in this.updatesPerKeyMap (use a hashmap)
     * 2 - build an iterator object that iterates over the records of the underling index
     * 3. for each key, check if it is present in the set (1)
     * 3a. if so, just return the fresh record as next. remember to remove it from the set
     * 3b. otherwise, return the object as it is
     */
    protected class MultiVersionIterator implements Iterator<Object[]>{

        private final Iterator<Map.Entry<IKey, Object[]>> freshSetIterator;

        public MultiVersionIterator(TransactionContext txCtx, IKey[] keys){
            this.freshSetIterator = collectFreshSet(txCtx, keys).entrySet().iterator();
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

    private Map<IKey, Object[]> collectFreshSet(TransactionContext txCtx, IKey[] keys) {
        Map<IKey, Object[]> freshSet = new HashMap<>();
        // iterate over keys
        for(IKey key : keys){
            var operation = this.updatesPerKeyMap.get(key);
            SingleWriterMultipleReadersFIFO.Entry<Long, TransactionWrite> obj = operation.updateHistoryMap.floorEntry(txCtx.tid);
            if (obj != null && obj.val().type != WriteType.DELETE) {
                freshSet.put(key, obj.val().record);
            }
        }
        return freshSet;
    }

    public Object[] getRecord(TransactionContext txCtx, IKey key){
        OperationSetOfKey operation = this.updatesPerKeyMap.get(key);
        if(operation != null){
            var entry = operation.updateHistoryMap.floorEntry(txCtx.tid);
            if(entry != null && entry.val().type != WriteType.DELETE) {
                return entry.val().record;
            }
        }
        return this.primaryKeyIndex.lookupByKey(key);
    }

    public SingleWriterMultipleReadersFIFO.Entry<Long, TransactionWrite> getFloorEntry(TransactionContext txCtx, IKey key){
        OperationSetOfKey operation = this.updatesPerKeyMap.get(key);
        return operation.updateHistoryMap.floorEntry(txCtx.tid);
    }

    /**
     * Used in full scan
     */
    @Override
    public Iterator<Object[]> iterator(TransactionContext txCtx) {
        return new PrimaryIndexIterator(txCtx);
    }

    private final class PrimaryIndexIterator implements Iterator<Object[]> {

        private final TransactionContext txCtx;
        private final Iterator<Map.Entry<IKey, OperationSetOfKey>> iterator;
        private Object[] currRecord;

        public PrimaryIndexIterator(TransactionContext txCtx){
            this.txCtx = txCtx;
            this.iterator = updatesPerKeyMap.entrySet().iterator();
        }

        @Override
        public boolean hasNext() {
            while(this.iterator.hasNext()){
                Map.Entry<IKey, OperationSetOfKey> next = this.iterator.next();
                var entry = next.getValue().updateHistoryMap.floorEntry(this.txCtx.tid);
                if(entry == null) {
                    this.currRecord = primaryKeyIndex.lookupByKey(next.getKey());
                    if(this.currRecord == null){
                        if(this.iterator.hasNext()) {
                            continue;
                        } else {
                            return false;
                        }
                    }
                    return true;
                }
                if(entry.val().type == WriteType.DELETE) {
                    if(this.iterator.hasNext()) {
                        continue;
                    } else {
                        return false;
                    }
                }
                this.currRecord = entry.val().record;
                return true;
            }
            return false;
        }

        @Override
        public Object[] next() {
            return this.currRecord;
        }

    }

}
