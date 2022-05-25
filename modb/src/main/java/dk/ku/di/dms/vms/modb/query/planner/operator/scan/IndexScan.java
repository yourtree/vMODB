package dk.ku.di.dms.vms.modb.query.planner.operator.scan;

import dk.ku.di.dms.vms.modb.query.planner.operator.result.RowOperatorResult;
import dk.ku.di.dms.vms.modb.query.planner.operator.filter.FilterInfo;
import dk.ku.di.dms.vms.modb.store.common.IKey;
import dk.ku.di.dms.vms.modb.store.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.common.meta.Row;

/**
 *
 */
public class IndexScan extends AbstractScan {

    private final IKey hashProbeKey;

    public IndexScan(AbstractIndex<IKey> index, IKey hashProbeKey, FilterInfo filterInfo) {
        super(index, filterInfo);
        this.hashProbeKey = hashProbeKey;
    }

    public IndexScan(AbstractIndex<IKey> index, IKey hashProbeKey) {
        super(index);
        this.hashProbeKey = hashProbeKey;
    }

    @Override
    public RowOperatorResult get() {

        Row row = index.retrieve( hashProbeKey );

        if(filters == null || check(row)){
            return buildResult(row);
        }

        // perhaps can avoid the overhead of creating the object
        return new RowOperatorResult(0);
    }

    private RowOperatorResult buildResult(Row row){
        RowOperatorResult result = new RowOperatorResult(1);
        result.accept( row );
        return result;
    }

}
