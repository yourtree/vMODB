package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionMetadata;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContextBuilder;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.FullScanWithProjection;
import dk.ku.di.dms.vms.modb.query.planner.operators.scan.IndexScanWithProjection;
import dk.ku.di.dms.vms.modb.transaction.multiversion.OperationSet;
import dk.ku.di.dms.vms.modb.transaction.multiversion.operation.DataItemVersion;
import dk.ku.di.dms.vms.modb.transaction.multiversion.operation.InsertOp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A transaction management facade
 * Responsibilities:
 * - Keep track of modifications
 * - Commit (write to the actual corresponding regions of memories)
 *
 * AbstractIndex must be modified so reads can return the correct (versioned/consistent) value
 *
 * Repository facade parses the request. Transaction facade deals with low-level operations
 *
 * Batch-commit aware. That means when a batch comes, must make data durable.
 */
public class TransactionFacade {

    private TransactionFacade(){}

    // key: tid
    private static final Map<Long, List<DataItemVersion>> writesPerTransaction;

    // key: PK
    private static final Map<IIndexKey, Map<IKey, OperationSet>> writesPerIndexAndKey;

    static {
        writesPerTransaction = new ConcurrentHashMap<>();
        writesPerIndexAndKey = new ConcurrentHashMap<>();
    }

    /****** ENTITY *******/

    public static void insert(Table table, Object[] values){

        // check constraints

        // pk -> lookup
        IKey pk = KeyUtils.buildRecordKey(table.getSchema(), table.getSchema().getPrimaryKeyColumns(), values);
        Map<IKey, OperationSet> keyMap = writesPerIndexAndKey.get( table.primaryKeyIndex().key() );

        boolean exception = false;
        if(keyMap == null){
            // no writes so far in this batch for this index

            // lets check now the index itself
            if(table.primaryKeyIndex().exists(pk)){
                exception = true;
            }

        } else if(keyMap.get(pk) != null) {
            exception = true;
            // check if this key is present in the keyMap
        }

        if(exception){

            long threadId = Thread.currentThread().getId();
            long tid = TransactionMetadata.tid(threadId);
            List<DataItemVersion> writesTid = writesPerTransaction.get(tid);

            if(writesTid == null){
                // throw right away
                throw new IllegalStateException("Primary key"+pk+"already exists for this table: "+table.getName());
            }

            // clean writes from this transaction

            for(DataItemVersion v : writesTid){

                // do we have a write in the corresponding index? always yes. if no, it is a bug
                Map<IKey, OperationSet> operations = writesPerIndexAndKey.get( v.indexKey() );
                if(operations != null){
                    operations.remove(v.pk());
                }

            }

            throw new IllegalStateException("Primary key"+pk+"already exists for this table: "+table.getName());
        }

        int recordSize = table.getSchema().getRecordSizeWithoutHeader();

        MemoryRefNode memRef = MemoryManager.getTemporaryDirectMemory(recordSize);

        long threadId = Thread.currentThread().getId();
        long tid = TransactionMetadata.tid(threadId);

        writesPerTransaction.putIfAbsent( tid, new ArrayList<>(10) );

        long addressToWriteTo = memRef.address();

        int maxColumns = table.getSchema().columnOffset().length;
        int index;

        // TODO For embed and default?, can be directly put in a buffer instead of saving an object.
        for(index = 0; index < maxColumns; index++) {

            DataType dt = table.getSchema().getColumnDataType(index);

            DataTypeUtils.callWriteFunction( addressToWriteTo,
                    dt,
                    values[index] );

            addressToWriteTo += dt.value;

        }

        DataItemVersion dataItemVersion = InsertOp.insert( tid, addressToWriteTo );

        writesPerTransaction.get( tid ).add( dataItemVersion );

    }

    /****** SCAN *******/

    public static MemoryRefNode run(List<WherePredicate> wherePredicates,
                                    IndexScanWithProjection operator){

        long threadId = Thread.currentThread().getId();
        long tid = TransactionMetadata.tid(threadId);

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

        Map<IKey, OperationSet> operationSetMap = writesPerIndexAndKey.get(operator.index.key());

        ConsistentView consistentView = new ConsistentView(operator.index, operationSetMap, tid);

        return operator.run( consistentView, filterContext, inputKey );
    }

    public static MemoryRefNode run(List<WherePredicate> wherePredicates,
                                    FullScanWithProjection operator){

        FilterContext filterContext = FilterContextBuilder.build(wherePredicates);

        Map<IKey, OperationSet> operationSetMap = writesPerIndexAndKey.get(operator.index.key());

        long threadId = Thread.currentThread().getId();
        long tid = TransactionMetadata.tid(threadId);

        ConsistentView consistentView = new ConsistentView(operator.index, operationSetMap, tid);

        return operator.run( consistentView, filterContext );

    }

}
