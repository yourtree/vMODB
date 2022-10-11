package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.common.constraint.ConstraintEnum;
import dk.ku.di.dms.vms.modb.common.constraint.ConstraintReference;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionId;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionMetadata;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Catalog;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.modb.index.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContextBuilder;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.FullScanWithProjection;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.IndexScanWithProjection;
import dk.ku.di.dms.vms.modb.transaction.multiversion.*;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static dk.ku.di.dms.vms.modb.common.constraint.ConstraintConstants.*;

/**
 * A transaction management facade
 * Responsibilities:
 * - Keep track of modifications
 * - Commit (write to the actual corresponding regions of memories)
 * AbstractIndex must be modified so reads can return the correct (versioned/consistent) value
 * Repository facade parses the request. Transaction facade deals with low-level operations
 * Batch-commit aware. That means when a batch comes, must make data durable.
 * in order to accommodate two or more VMSs in the same resource,
 *  it would need to make this class an instance (no static methods) and put it into modb modules
 */
public class TransactionFacade {

    // most transactions write to maximum 2 tables
    private static final int DEFAULT_NUM_INDEX_PER_TRANSACTION = 2;

    // most transactions will write maximum 2 keys per index
    private static final int DEFAULT_NUM_KEY_PER_INDEX_IN_A_TRANSACTION = 2;

    private TransactionFacade(){}

    /**
     * It is necessary to keep track of the changes
     * key: tid. value: the version installed by this transaction
     * single-thread, no need to synchronize.
     * It only maintains the last write per key for a transaction.
     */
    private static final Map<TransactionId, Map<IIndexKey, Map<IKey, DataItemVersion>>> writesPerTransaction;

    /**
     * Why operation set of key contains only delete and insert?
     * Because these are operations that cannot occur twice.
     * The single-thread model safeguards interleavings, which means
     * that any new RW task that intends to delete or insert a given key,
     * can be simply checked without taking into consideration the transaction ID
     */
    private static final Map<IIndexKey, Map<IKey, OperationSetOfKey>> writesPerIndexAndKey;

    static {
        writesPerTransaction = new ConcurrentHashMap<>();
        writesPerIndexAndKey = new ConcurrentHashMap<>();
    }

    /**
     * It installs the writes without taking into consideration concurrency control.
     * Used when the buffer received is aligned with how the data is stored in memory
     */
    public static void bulkInsert(Table table, ByteBuffer buffer, int numberOfRecords){
        // if the memory address is occupied, must log warning
        // so we can increase the table size
        long address = MemoryUtils.getByteBufferAddress(buffer);
        bulkInsert(table, address, numberOfRecords);
    }

    public static void bulkInsert(Table table, long srcAddress, int numberOfRecords){

        // if the memory address is occupied, must log warning
        // so we can increase the table size
        AbstractIndex<IKey> index = table.primaryKeyIndex();
        int sizeWithoutHeader = table.schema.getRecordSizeWithoutHeader();
        long currAddress = srcAddress;

        IKey key;
        for (int i = 0; i < numberOfRecords; i++) {
            key = KeyUtils.buildPrimaryKey(table.schema, currAddress);
            index.insert( key, currAddress );
            currAddress += sizeWithoutHeader;
        }

    }

    /****** ENTITY *******/

    public static void insertAll(Table table, List<Object[]> objects){
        insertAll(table, objects, true);
    }

    /**
     *
     * @param table the table to insert
     * @param objects the parsed objects
     * @param transactional whether it should write to the multi-versioning scheme or directly to the index entries
     */
    public static void insertAll(Table table, List<Object[]> objects, boolean transactional){

        // get tid, do all the checks, etc
        if(transactional){
            for(Object[] entry : objects){
                insert(table, entry);
            }
        }

        int bufferSize = table.getSchema().getRecordSizeWithoutHeader() * objects.size();
        MemoryRefNode memoryRefNode = MemoryManager.getTemporaryDirectMemory( bufferSize );

        long currAddress = memoryRefNode.address;

        for(Object[] entry : objects){
            currAddress = writeToMemory( table, entry, currAddress );
        }

        bulkInsert(table, memoryRefNode.address, objects.size());

    }

    public static void delete(Table table, Object[] values) {
        IKey pk = KeyUtils.buildPrimaryKey(table.getSchema(), values);
        deleteByKey(table, pk);
    }

    public static void deleteByKey(Table table, Object[] values) {
        IKey pk = KeyUtils.buildPrimaryKey(table.getSchema(), values);
        deleteByKey(table, pk);
    }

    /**
     * TODO not considering foreign key
     * @param table The corresponding database table
     * @param pk The primary key
     */
    private static void deleteByKey(Table table, IKey pk){

        IIndexKey indexKey = table.primaryKeyIndex().key();
        Map<IKey, OperationSetOfKey> keyMap = writesPerIndexAndKey.get( indexKey );

        // if != null, there may have been an insert of this key
        if(keyMap != null && keyMap.get(pk) != null){

            OperationSetOfKey operationSet = keyMap.get(pk);

            if(operationSet.lastWriteType != WriteType.DELETE){

                long threadId = Thread.currentThread().getId();
                TransactionId tid = TransactionMetadata.tid(threadId);

                TransactionHistoryEntry entry = new TransactionHistoryEntry(WriteType.DELETE, null);
                DataItemVersion deleteVersion = new DataItemVersion( indexKey, pk, entry );

                includeWriteInTransactionHistory( tid, deleteVersion );

                //includeHistoryEntryForKey( tid, entry, operationSet );

            }

            // does this key even exist? if not, don't even need to save it on transaction metadata
        } else if(table.primaryKeyIndex().exists(pk)) {

            long threadId = Thread.currentThread().getId();
            TransactionId tid = TransactionMetadata.tid(threadId);

            TransactionHistoryEntry entry = new TransactionHistoryEntry(WriteType.DELETE, null);
            DataItemVersion deleteVersion = new DataItemVersion( indexKey, pk, entry );

            includeWriteInTransactionHistory( tid, deleteVersion );

            //includeWriteInTheSetOfOperationsForAnIndexKey(pk, keyMap, indexKey, tid, entry );

        }

    }

    public static Object[] lookupByKey(Table table, Object... valuesOfKey){

        IKey pk = KeyUtils.buildPrimaryKeyFromKeyValues(valuesOfKey);
        IIndexKey indexKey = table.primaryKeyIndex().key();
        Map<IKey, OperationSetOfKey> keyMap = writesPerIndexAndKey.get( table.primaryKeyIndex().key() );

        if(keyMap != null && keyMap.get(pk) != null){

            long threadId = Thread.currentThread().getId();
            TransactionId tid = TransactionMetadata.tid( threadId );

            OperationSetOfKey operationSet = keyMap.get(pk);

            // read your writes
            if(writesPerTransaction.get( tid ) != null &&
                    writesPerTransaction.get( tid ).get( indexKey ) != null &&
                        writesPerTransaction.get( tid ).get( indexKey ).get( pk ) != null){

                // this tid has performed at least one write
                var write = writesPerTransaction.get( tid ).get( indexKey ).get( pk );

                if( write != null ){
                    if ( write.entry.type == WriteType.DELETE ) return null;
                    return write.entry.record;
                }

            }

            // since I have not written anything to this key, we have to check the last update for this key
            TransactionId previousTid = TransactionMetadata.getPreviousWriteTransaction( threadId );
            var lastUpdateEntry = operationSet.updateHistoryMap.floorEntry(previousTid);
            if( lastUpdateEntry != null ){
                if(lastUpdateEntry.getValue().type == WriteType.DELETE) return null;
                return lastUpdateEntry.getValue().record;
            } else {
                return readRecordFromIndex(table, pk);
            }

        } else {
            return readRecordFromIndex(table, pk);
        }

    }

    private static Object[] readRecordFromIndex(Table table, IKey pk) {
        if (table.primaryKeyIndex().exists(pk)){
            long address = table.primaryKeyIndex().retrieve(pk);
            return table.primaryKeyIndex().readFromIndex(address);
        }
        return null;
    }

    /**
     * Called when a constraint is violated, leading to a transaction abort
     * @param tid transaction id
     */
    private static void undoTransactionWrites(TransactionId tid){

        var writesOfTid = writesPerTransaction.get(tid).entrySet();

        for(var indexKeyEntry : writesOfTid) {
            for (var write : indexKeyEntry.getValue().entrySet()) {

                // do we have a write in the corresponding index? always yes. if no, it is a bug
                Map<IKey, OperationSetOfKey> operationsOfIndex = writesPerIndexAndKey.get(write.getValue().indexKey());
                OperationSetOfKey operationSetOfKey = operationsOfIndex.get(write.getValue().pk());

                // get previous TID, not necessarily the previous TID. The lastTID may have not written to this key.
                var entry = operationSetOfKey.updateHistoryMap.lowerEntry(tid);
                if (entry != null) {
                    operationSetOfKey.cachedEntity = entry.getValue().record;
                    operationSetOfKey.lastWriteType = entry.getValue().type;
                } else {
                    operationSetOfKey.cachedEntity = null;
                    operationSetOfKey.lastWriteType = null;
                }

                operationSetOfKey.updateHistoryMap.remove(tid);

            }
        }

    }

    /**
     * TODO if cached value is not null, then extract the updated columns to make constraint violation check faster
     * @param table The corresponding database table
     * @param values The fields extracted from the entity
     */
    public static void insert(Table table, Object[] values){

        IKey pk = KeyUtils.buildRecordKey(table.getSchema().getPrimaryKeyColumns(), values);
        IIndexKey indexKey = table.primaryKeyIndex().key();

        long threadId = Thread.currentThread().getId();
        TransactionId tid = TransactionMetadata.tid(threadId);

        if(pkConstraintViolationForInsert(tid, table.primaryKeyIndex(), pk) ||
                nonPkConstraintViolation(table, values) ||
                    fkConstraintViolation(tid, table, values)){
            undoTransactionWrites(tid);
            throw new IllegalStateException("Primary key"+pk+"already exists for this table: "+table.getName());
        }

        // create a new insert
        TransactionHistoryEntry entry = new TransactionHistoryEntry(WriteType.INSERT, values);
        DataItemVersion dataItemVersion = new DataItemVersion( indexKey, pk, entry );

        // update writes per transaction
        includeWriteInTransactionHistory(tid, dataItemVersion);

    }

    /**
     * TODO check own writes first, before checking the writes of the others...
     * @param table
     * @param values
     */
    public static void update(Table table, Object[] values){

        IKey pk = KeyUtils.buildRecordKey(table.getSchema().getPrimaryKeyColumns(), values);
        IIndexKey indexKey = table.primaryKeyIndex().key();

        long threadId = Thread.currentThread().getId();
        TransactionId tid = TransactionMetadata.tid(threadId);

        if(pkConstraintViolationForUpdate(tid, table.primaryKeyIndex(), pk) ||
                nonPkConstraintViolation(table, values) ||
                    fkConstraintViolation(tid, table, values)){
            undoTransactionWrites(tid);
            throw new IllegalStateException("Primary key"+pk+"already exists for this table: "+table.getName());
        }

        // create a new update
        TransactionHistoryEntry entry = new TransactionHistoryEntry(WriteType.UPDATE, values);
        DataItemVersion dataItemVersion = new DataItemVersion( indexKey, pk, entry );

        // update writes per transaction
        includeWriteInTransactionHistory(tid, dataItemVersion);

    }

    private static void includeWriteInTheSetOfOperationsForAnIndexKey(IKey pk, Map<IKey, OperationSetOfKey> keyMap, IIndexKey indexKey,
                                                                      TransactionId transactionId, TransactionHistoryEntry entry) {

        OperationSetOfKey operationSet;

        if(keyMap == null){

            // let's create the entry for this indexKey
            keyMap = new ConcurrentHashMap<>();
            writesPerIndexAndKey.put(indexKey, keyMap);

            // that means we haven't had any previous transaction performing writes to this key
            operationSet = new OperationSetOfKey();
            keyMap.put(pk, operationSet);

            includeHistoryEntryForKey(transactionId, entry, operationSet);

        } else {
            operationSet = keyMap.get(pk);

            // the map for this index may have been created but no entry created for this key
            if(operationSet == null){
                operationSet = new OperationSetOfKey();
                keyMap.put(pk, operationSet);
            }

            includeHistoryEntryForKey(transactionId, entry, operationSet);

        }

    }

    private static void includeHistoryEntryForKey(TransactionId tid, TransactionHistoryEntry entry,
                                                  OperationSetOfKey operationSet) {

        // let's get the last and update the entry
//        TransactionHistoryEntry prev = operationSet.updateHistoryMap.get(tid);

//        if (prev != null) {
//            // swap O(1)
//            TransactionHistoryEntry newPrev = new TransactionHistoryEntry(prev.type, prev.record);
//            prev.type = writeType;
//            prev.record = values;
//            prev.previous = newPrev;
//        } else {
            // insert in the map O(log n)
        // TODO this insert can be performed when the transaction finishes to allow for faster return to app code. this write in this map is not going to be used by this TID, only by subsequent tasks
        //  we save a log n insertion...
            operationSet.updateHistoryMap.put(tid, entry);
//        }


        // operationSet.deleteInsertCacheMap.put(tid, entry);

        operationSet.lastWriteType = entry.type;
        operationSet.cachedEntity = entry.record;

    }

    /**
     * O(1)
     */
    private static void includeWriteInTransactionHistory(TransactionId tid, DataItemVersion dataItemVersion) {
        var indexMapForTid = writesPerTransaction.putIfAbsent(tid, new HashMap<>(DEFAULT_NUM_INDEX_PER_TRANSACTION));
        assert indexMapForTid != null;
        var keyMapForTid = indexMapForTid.putIfAbsent( dataItemVersion.indexKey(), new HashMap<>(DEFAULT_NUM_KEY_PER_INDEX_IN_A_TRANSACTION) );
        assert keyMapForTid != null;
        keyMapForTid.put( dataItemVersion.pk(), dataItemVersion );
    }

    /****** OBJECT AND MEMORY MANAGEMENT *******/

    private static long writeToMemory(Table table, Object[] values, long address){

        int maxColumns = table.getSchema().columnOffset().length;
        long currAddress = address;

        for(int index = 0; index < maxColumns; index++) {

            DataType dt = table.getSchema().getColumnDataType(index);

            DataTypeUtils.callWriteFunction( currAddress,
                    dt,
                    values[index] );

            currAddress += dt.value;

        }

        return currAddress;

    }

    /****** CONSTRAINT UTILS *******/

    private static boolean fkConstraintViolation(TransactionId tid, Table table, Object[] values){

        for(var entry : table.foreignKeysGroupedByTableMap.entrySet()){

            // believed to be the PK of the other table
            IKey fk = KeyUtils.buildRecordKey( entry.getValue(), values );

            // have this transaction deleted this key?
            boolean hasThisTidDeletedThisFK = writesPerTransaction.get(tid) != null &&
                    writesPerTransaction.get(tid).get(entry.getKey().primaryKeyIndex().key()) != null &&
                    writesPerTransaction.get(tid).get(entry.getKey().primaryKeyIndex().key()).get(fk) != null &&
                    writesPerTransaction.get(tid).get(entry.getKey().primaryKeyIndex().key()).get(fk).entry.type == WriteType.DELETE;

            if(hasThisTidDeletedThisFK) return true;

            // have some previous TID deleted it?
            boolean hasSomeTidDeletedThisFK = writesPerIndexAndKey.get( entry.getKey().primaryKeyIndex().key() ) != null &&
                    writesPerIndexAndKey.get( entry.getKey().primaryKeyIndex().key() ).get( fk ) != null &&
                    writesPerIndexAndKey.get( entry.getKey().primaryKeyIndex().key() ).get( fk ).lastWriteType == WriteType.DELETE;

            if(hasSomeTidDeletedThisFK) return true;

        }

        return false;

    }

    /**
     * Method for insert operations.
     */
    private static boolean pkConstraintViolationForInsert(TransactionId tid, AbstractIndex<IKey> index, IKey pk){

        // if there is an entry for this key, must be a DELETE. if not, it is a violation
        boolean hasThisTidWroteThisPK = writesPerTransaction.get(tid) != null &&
                writesPerTransaction.get(tid).get(index.key()) != null &&
                writesPerTransaction.get(tid).get(index.key()).get(pk) != null &&
                writesPerTransaction.get(tid).get(index.key()).get(pk).entry.type != WriteType.DELETE;

        if(hasThisTidWroteThisPK) return true;

        Map<IKey, OperationSetOfKey> keyMap = writesPerIndexAndKey.get( index.key() );

        if(keyMap != null && keyMap.get(pk) != null){
            // entering this block means we have updates for this PK
            OperationSetOfKey operationSetOfKey = keyMap.get(pk);
            // does the last write is not a delete?
            return operationSetOfKey.lastWriteType != WriteType.DELETE; // if not delete, violation (it means some tid has written to this PK before)
        }
        // lets check now the index itself. it exist, it is a violation
        return index.exists(pk);
    }

    private static boolean pkConstraintViolationForUpdate(TransactionId tid, AbstractIndex<IKey> index, IKey pk){

        // if there is an entry for this key, must not be a DELETE.
        boolean hasThisTidDeletedThisPK = writesPerTransaction.get(tid) != null &&
                writesPerTransaction.get(tid).get(index.key()) != null &&
                writesPerTransaction.get(tid).get(index.key()).get(pk) != null &&
                writesPerTransaction.get(tid).get(index.key()).get(pk).entry.type == WriteType.DELETE;

        if(hasThisTidDeletedThisPK) return true;

        Map<IKey, OperationSetOfKey> keyMap = writesPerIndexAndKey.get( index.key() );

        if(keyMap != null && keyMap.get(pk) != null){
            OperationSetOfKey operationSetOfKey = keyMap.get(pk);
            // does the last write is a delete?
            return operationSetOfKey.lastWriteType == WriteType.DELETE;
        }
        // if not exists, violation. cannot update something that does not exist
        return !index.exists(pk);
    }

    private static boolean nonPkConstraintViolation(Table table, Object[] values) {

        Map<Integer, ConstraintReference> constraints = table.getSchema().constraints();

        boolean violation = false;

        for(Map.Entry<Integer, ConstraintReference> c : constraints.entrySet()) {

            switch (c.getValue().constraint.type){

                case NUMBER -> {
                    switch (table.getSchema().getColumnDataType(c.getKey())) {
                        case INT -> violation = NumberTypeConstraintHelper.eval((int)values[c.getKey()] , 0, Integer::compareTo, c.getValue().constraint);
                        case LONG, DATE -> violation = NumberTypeConstraintHelper.eval((long)values[c.getKey()] , 0L, Long::compareTo, c.getValue().constraint);
                        case FLOAT -> violation = NumberTypeConstraintHelper.eval((float)values[c.getKey()] , 0f, Float::compareTo, c.getValue().constraint);
                        case DOUBLE -> violation = NumberTypeConstraintHelper.eval((double)values[c.getKey()] , 0d, Double::compareTo, c.getValue().constraint);
                        default -> throw new IllegalStateException("Data type "+c.getValue().constraint.type+" cannot be applied to a number");
                    }
                }

                case NUMBER_WITH_VALUE -> {
                    Object valToCompare = c.getValue().asValueConstraint().value;
                    switch (table.getSchema().getColumnDataType(c.getKey())) {
                        case INT -> violation = NumberTypeConstraintHelper.eval((int)values[c.getKey()] , (int)valToCompare, Integer::compareTo, c.getValue().constraint);
                        case LONG, DATE -> violation = NumberTypeConstraintHelper.eval((long)values[c.getKey()] , (long)valToCompare, Long::compareTo, c.getValue().constraint);
                        case FLOAT -> violation = NumberTypeConstraintHelper.eval((float)values[c.getKey()] , (float)valToCompare, Float::compareTo, c.getValue().constraint);
                        case DOUBLE -> violation = NumberTypeConstraintHelper.eval((double)values[c.getKey()] , (double)valToCompare, Double::compareTo, c.getValue().constraint);
                        default -> throw new IllegalStateException("Data type "+c.getValue().constraint.type+" cannot be applied to a number");
                    }
                }

                case NULLABLE ->
                        violation = NullableTypeConstraintHelper.eval(values[c.getKey()], c.getValue().constraint);

                case CHARACTER ->
                    violation = CharOrStringTypeConstraintHelper.eval((String) values[c.getKey()], c.getValue().constraint );

            }

            if(violation) return true;

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

    /****** SCAN OPERATORS *******/

    public static MemoryRefNode run(List<WherePredicate> wherePredicates,
                                    IndexScanWithProjection operator){

        long threadId = Thread.currentThread().getId();
        TransactionId tid = TransactionMetadata.tid(threadId);
        TransactionId lastTid = TransactionMetadata.getPreviousWriteTransaction( threadId );

        List<Object> keyList = new ArrayList<>(operator.index.columns().length);
        List<WherePredicate> wherePredicatesNoIndex = new ArrayList<>(wherePredicates.size());
        // build filters for only those columns not in selected index
        for (WherePredicate wherePredicate : wherePredicates) {
            // not found, then build filter
            if(operator.index.columnsHash().contains( wherePredicate.columnReference.columnPosition )){
                keyList.add( wherePredicate.value );
            } else {
                wherePredicatesNoIndex.add(wherePredicate);
            }
        }

        FilterContext filterContext = FilterContextBuilder.build(wherePredicatesNoIndex);

        // build input
        IKey inputKey = KeyUtils.buildInputKey(keyList.toArray());

        Map<IKey, OperationSetOfKey> operationSetMap = writesPerIndexAndKey.get(operator.index.key());

        ConsistentView consistentView = new ConsistentView(operator.index,
                writesPerTransaction.get( tid ) != null ? writesPerTransaction.get( tid ).get( operator.index.key() ) : null,
                operationSetMap, lastTid);

        return operator.run( consistentView, filterContext, inputKey );
    }

    public static MemoryRefNode run(List<WherePredicate> wherePredicates,
                                    FullScanWithProjection operator){

        FilterContext filterContext = FilterContextBuilder.build(wherePredicates);

        Map<IKey, OperationSetOfKey> operationSetMap = writesPerIndexAndKey.get(operator.index.key());

        long threadId = Thread.currentThread().getId();
        TransactionId tid = TransactionMetadata.tid(threadId);
        TransactionId lastTid = TransactionMetadata.getPreviousWriteTransaction( threadId );

        ConsistentView consistentView = new ConsistentView(operator.index,
                writesPerTransaction.get( tid ) != null ? writesPerTransaction.get( tid ).get( operator.index.key() ) : null,
                operationSetMap, lastTid);

        return operator.run( consistentView, filterContext );

    }

    /* CHECKPOINTING *******/

    /**
     * Only log those data versions until the corresponding batch.
     * TIDs are not necessarily a sequence.
     * TODO merge the last values of each data item to avoid multiple writes
     *      use virtual threads to speed up
     *
     * @param lastTid the last tid of the batch
     */
    public static void log(long lastTid, Catalog catalog){

        // make state durable
        // get buffered writes in transaction facade and merge in memory

        for(var entry : writesPerIndexAndKey.entrySet()){

            // for each index, get the last update
            ReadWriteIndex<IKey> index = catalog.getIndexByKey(entry.getKey());

            for(var keyEntry : entry.getValue().entrySet()){

                switch (keyEntry.getValue().lastWriteType){

                    case INSERT -> {
                    }

                    case DELETE -> {
                        // put bit active as 0
                    }

                    case UPDATE -> {
                        // memory copy

                    }

                }

            }



            // log index since all updates are made
            index.asUniqueHashIndex().buffer().log();

            writesPerIndexAndKey.remove(entry.getKey());

        }


        for(var tx : writesPerTransaction.entrySet()){
//            if(tx.getKey() > lastTid) break; // can stop
            writesPerTransaction.remove( tx.getKey() );
        }

        // TODO must modify corresponding secondary indexes too

    }

}
