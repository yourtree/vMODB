package dk.ku.di.dms.vms.database.query.planner.node.scan;

import dk.ku.di.dms.vms.database.query.planner.OperatorResult;
import dk.ku.di.dms.vms.database.query.planner.node.filter.FilterInfo;
import dk.ku.di.dms.vms.database.query.planner.node.filter.IFilter;
import dk.ku.di.dms.vms.database.query.planner.utils.IdentifiableNode;
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
public final class SequentialScan implements Supplier<OperatorResult> {

    /**
     * The index to iterate with
     */
    private final AbstractIndex index;

    /**
     * Filters are agnostic to the columns they are accessing,
     * so they can be reused more easily
     */
    private final IFilter[] filters;

    /**
     * The scan operator requires the column to be accessed for each filter
     */
    private final int[] filterColumns;

    /** The parameters of the filters */
    private Collection<IdentifiableNode<Object>> filterParams;

    public SequentialScan(final AbstractIndex index,
                          final FilterInfo filterInfo) {
        this.index = index;
        this.filters = filterInfo.filters;
        this.filterColumns = filterInfo.filterColumns;
        this.filterParams = filterInfo.filterParams;
    }

    /**
     * No filter scan. For instance, SELECT * FROM table
     * @param index
     */
    public SequentialScan(final AbstractIndex index) {
        this.index = index;
        this.filters = null;
        this.filterColumns = null;
        this.filterParams = null;
    }

    @Override
    @SuppressWarnings("unchecked")
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

    private boolean check(final Row row){

        boolean conditionHolds = true;
        int filterIdx = 0;

        IFilter currFilter;

        Iterator<IdentifiableNode<Object>> paramsIterator = filterParams.iterator();
        IdentifiableNode currParam = null;
        if (paramsIterator.hasNext()){
            currParam = paramsIterator.next();
        }

        while( conditionHolds && filterIdx < filters.length ){
            currFilter = filters[filterIdx];

            // unchecked cast, but we know it is safe since the analyzer makes sure that
            if(currParam != null && currParam.id == filterIdx) {
                conditionHolds = currFilter.eval(row.get(filterColumns[filterIdx]), currParam );
                if (paramsIterator.hasNext()){
                    currParam = paramsIterator.next();
                } else {
                    currParam = null;
                }
            }
            else {
                conditionHolds = currFilter.eval(row.get(filterColumns[filterIdx]));
            }

            // no need to continue anymore
            if(!conditionHolds) break;

            filterIdx++;

        }

        return conditionHolds;

    }

}
