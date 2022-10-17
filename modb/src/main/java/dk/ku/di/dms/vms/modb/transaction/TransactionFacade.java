package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionId;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionMetadata;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContextBuilder;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.FullScanWithProjection;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.IndexScanWithProjection;
import dk.ku.di.dms.vms.modb.transaction.multiversion.ConsistentIndex;

import java.nio.ByteBuffer;
import java.util.*;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.R;

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
public final class TransactionFacade {

    private static final ThreadLocal<Set<ConsistentIndex>> INDEX_WRITES = ThreadLocal.withInitial( () -> {
        if(TransactionMetadata.TRANSACTION_CONTEXT.get().type != R) {
            return new HashSet<>(2);
        }
        return Collections.emptySet();
    });

    private TransactionFacade(){}

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

    public static void insertAll(ConsistentIndex index, List<Object[]> objects){
        // get tid, do all the checks, etc
        for(Object[] entry : objects) {
            insert(index, entry);
        }
    }

    public static void delete(ConsistentIndex index, Object[] values) {
        IKey pk = KeyUtils.buildPrimaryKey(index.schema(), values);
        deleteByKey(index, pk);
    }

    public static void deleteByKey(ConsistentIndex index, Object[] values) {
        IKey pk = KeyUtils.buildInputKey(values);
        deleteByKey(index, pk);
    }

    /**
     * TODO not considering foreign key. must prune the other tables inside this VMS
     * @param index The corresponding database index
     * @param pk The primary key
     */
    private static void deleteByKey(ConsistentIndex index, IKey pk){
        if(index.delete(pk)){
            INDEX_WRITES.get().add(index);
        }
    }

    public static Object[] lookupByKey(ConsistentIndex index, Object... valuesOfKey){
        IKey pk = KeyUtils.buildPrimaryKeyFromKeyValues(valuesOfKey);
        return index.lookupByKey(pk);
    }

    /**
     * @param index The corresponding database table
     * @param values The fields extracted from the entity
     */
    public static void insert(ConsistentIndex index, Object[] values){
        IKey pk = KeyUtils.buildRecordKey(index.schema().getPrimaryKeyColumns(), values);
        if(!index.insert(pk, values)) {
            INDEX_WRITES.set(null);
            index.undoTransactionWrites();
            throw new RuntimeException("Constraint violation.");
        }
        INDEX_WRITES.get().add(index);
    }

    /**
     * iterate over all indexes, get the corresponding writes of this tid and remove them
     *      this method can be called in parallel by transaction facade without risk
     */
    public static void update(ConsistentIndex index, Object[] values){
        IKey pk = KeyUtils.buildRecordKey(index.schema().getPrimaryKeyColumns(), values);
        if(!index.update(pk, values)){
            index.undoTransactionWrites();
            throw new RuntimeException("Constraint violation.");
        }
        INDEX_WRITES.get().add(index);
    }

    /****** SCAN OPERATORS *******/

    public static MemoryRefNode run(ConsistentIndex consistentIndex,
                                    List<WherePredicate> wherePredicates,
                                    IndexScanWithProjection operator){

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

        return operator.run( consistentIndex, filterContext, inputKey );
    }

    public static MemoryRefNode run(ConsistentIndex consistentIndex,
                                    List<WherePredicate> wherePredicates,
                                    FullScanWithProjection operator){
        FilterContext filterContext = FilterContextBuilder.build(wherePredicates);
        return operator.run( consistentIndex, filterContext );
    }

    /* CHECKPOINTING *******/

    /**
     * Only log those data versions until the corresponding batch.
     * TIDs are not necessarily a sequence.
     */
    public static void commit(){

        // make state durable
        // get buffered writes in transaction facade and merge in memory
        var indexes = INDEX_WRITES.get();

        for(var index : indexes){
            index.installWrites();
            // log index since all updates are made
            // index.asUniqueHashIndex().buffer().log();
        }

        // TODO must modify corresponding secondary indexes too

    }

}
