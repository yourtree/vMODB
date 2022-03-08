package dk.ku.di.dms.vms.database.query.planner.node.scan;

import dk.ku.di.dms.vms.database.query.planner.OperatorResult;
import dk.ku.di.dms.vms.database.query.planner.node.filter.FilterInfo;
import dk.ku.di.dms.vms.database.query.planner.node.filter.IFilter;
import dk.ku.di.dms.vms.database.query.planner.utils.IdentifiableNode;
import dk.ku.di.dms.vms.database.store.common.IKey;
import dk.ku.di.dms.vms.database.store.index.AbstractIndex;
import dk.ku.di.dms.vms.database.store.row.Row;
import dk.ku.di.dms.vms.database.store.table.Table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Supplier;

/**
 * aka table scan
 * A thread should run the scan
 * This is a blocking implementation, i.e.,
 * the upstream operator must wait for this entire execution to get to work
 */
public final class SequentialScan extends AbstractScan {

    public SequentialScan(AbstractIndex<IKey> index, FilterInfo filterInfo) {
        super(index, filterInfo);
    }

    public SequentialScan(AbstractIndex<IKey> index) {
        super(index);
    }

    @Override
    public OperatorResult get() {

        final boolean noFilter = filters == null;

        if (noFilter) {
            return new OperatorResult(index.rows());
        }

        // it avoids resizing in most cases array
        OperatorResult result = new OperatorResult(index.rows().size());

        Collection<Row> rows = index.rows();

        for(Row row : rows){
            if(check(row)) {
                result.accept(row);
            }
        }

        return result;
    }

}
