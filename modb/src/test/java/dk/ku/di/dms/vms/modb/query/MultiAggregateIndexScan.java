package dk.ku.di.dms.vms.modb.query;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.GroupByPredicate;
import dk.ku.di.dms.vms.modb.query.execution.operators.AbstractSimpleOperator;
import dk.ku.di.dms.vms.modb.transaction.TransactionContext;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Can be reused for the same query
 */
public final class MultiAggregateIndexScan extends AbstractSimpleOperator {

    private final GroupByPredicate[] aggregations;
    private final IMultiVersionIndex index;
    private final int[] projectionColumns;

    public MultiAggregateIndexScan(GroupByPredicate[] aggregations,
                                   IMultiVersionIndex index,
                                   // only the ones that are not aggregation
                                   int[] projectionColumns,
                                   int entrySize) {
        super(entrySize);
        this.aggregations = aggregations;
        this.index = index;
        this.projectionColumns = projectionColumns;
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx, IKey key){

        Object[] baseRecord = new Object[this.projectionColumns.length];

        Iterator<Object[]> iterator = this.index.iterator(txCtx, key);
        while(iterator.hasNext()){
            this.compute( iterator.next() );
        }
        var res = new ArrayList<Object[]>();
        res.add(baseRecord);
        return res;
    }

    private void compute(Object[] record){

    }

}
