package dk.ku.di.dms.vms.modb.query.planner.operator.scan;

import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.query.planner.operator.result.RowOperatorResult;
import dk.ku.di.dms.vms.modb.query.planner.operator.filter.FilterInfo;
import dk.ku.di.dms.vms.modb.schema.key.IKey;

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
    public RowOperatorResult get() {

//        final boolean noFilter = filters == null;
//
//        if (noFilter) {
//            return new RowOperatorResult(index.rows());
//        }
//
//        // it avoids resizing in most cases array
//        RowOperatorResult result = new RowOperatorResult(index.rows().size());
//
//        Collection<Row> rows = index.rows();
//
//        for(Row row : rows){
//            if(check(row)) {
//                result.accept(row);
//            }
//        }
//
//        return result;
        return null;
    }

}
