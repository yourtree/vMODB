package dk.ku.di.dms.vms.modb.query.execution.operators;

import dk.ku.di.dms.vms.modb.query.execution.operators.join.UniqueHashJoinWithProjection;
import dk.ku.di.dms.vms.modb.query.execution.operators.min.IndexGroupByMinWithProjection;
import dk.ku.di.dms.vms.modb.query.execution.operators.scan.AbstractScan;
import dk.ku.di.dms.vms.modb.query.execution.operators.scan.FullScanWithProjection;
import dk.ku.di.dms.vms.modb.query.execution.operators.scan.IndexScanWithProjection;

/**
 * Used for simple queries.
 * This can speed up most OLTP workloads because the number of function calls
 * is reduced, since there is no data being passed along different operators.
 */
public abstract class AbstractSimpleOperator {

    protected final int entrySize;

    public AbstractSimpleOperator(int entrySize) {
        this.entrySize = entrySize;
    }

    // must be overridden by the concrete operators
    public boolean isFullScan(){
        return false;
    }

    public boolean isIndexAggregationScan(){
        return false;
    }

    public boolean isIndexMultiAggregationScan(){
        return false;
    }

    public boolean isIndexScan(){
        return false;
    }

    public boolean isHashJoin() { return false; }

    public IndexGroupByMinWithProjection asIndexAggregationScan(){
        throw new IllegalStateException("No index scan operator");
    }

    public IndexMultiAggregateScan asIndexMultiAggregationScan(){
        throw new IllegalStateException("No index scan operator");
    }

    public IndexScanWithProjection asIndexScan(){
        throw new IllegalStateException("No index scan operator");
    }

    public FullScanWithProjection asFullScan(){
        throw new IllegalStateException("No full scan operator");
    }

    public AbstractScan asScan(){
        throw new IllegalStateException("No abstract scan operator");
    }

    public UniqueHashJoinWithProjection asHashJoin() { throw new IllegalStateException("No hash join operator"); }

}
