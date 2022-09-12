package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.transaction.TransactionMetadata;
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

import java.util.*;
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
    private static final Map<long, List<DataItemVersion>> writesPerTransaction;

    // key: PK
    private static final Map<IIndexKey, Map<IKey, OperationSet>> writesPerIndexAndKey;

    static {
        writesPerTransaction = new ConcurrentHashMap<long,List<DataItemVersion>>();
        writesPerIndexAndKey = new ConcurrentHashMap<>();
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
