package dk.ku.di.dms.vms.database.query.planner.node.scan;

import dk.ku.di.dms.vms.database.query.planner.OperatorResult;
import dk.ku.di.dms.vms.database.query.planner.node.filter.FilterInfo;
import dk.ku.di.dms.vms.database.store.common.IKey;
import dk.ku.di.dms.vms.database.store.index.AbstractIndex;
import dk.ku.di.dms.vms.database.store.row.Row;

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
    public OperatorResult get() {

        Row row = index.retrieve( hashProbeKey );

        if(filters == null || check(row)){
            buildResult(row);
        }

        // perhaps can avoid the overhead of creating the object
        return new OperatorResult(0);
    }

    private OperatorResult buildResult(Row row){
        OperatorResult result = new OperatorResult(1);
        result.accept( row );
        return result;
    }

}
