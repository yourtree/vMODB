package dk.ku.di.dms.vms.database.query.planner.node.scan;

import dk.ku.di.dms.vms.database.query.planner.OperatorResult;
import dk.ku.di.dms.vms.database.query.planner.node.filter.IFilter;
import dk.ku.di.dms.vms.database.query.planner.utils.IdentifiableNode;
import dk.ku.di.dms.vms.database.store.row.Row;
import dk.ku.di.dms.vms.database.store.table.Table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * aka table scan
 * A thread should run the scan
 */
public final class SequentialScan implements Supplier<OperatorResult> {

    /**
     * The table to iterate with
     */
    private final Table table;

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

    public SequentialScan(final Table table,
                          final IFilter[] filters,
                          final int[] filterColumns,
                          final Collection<IdentifiableNode<Object>> filterParams)
    {
        this.table = table;
        this.filters = filters;
        this.filterColumns = filterColumns;
        this.filterParams = filterParams;
    }

    /**
     * No filter scan. For instance, SELECT * FROM table
     * @param table
     */
    public SequentialScan(final Table table) {
        this.table = table;
        this.filters = null;
        this.filterColumns = null;
        this.filterParams = null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public OperatorResult get() {

        final boolean noFilter = filters == null;
        OperatorResult res = new OperatorResult();

        if (noFilter) {
            res.rows = table.rows();
            return res;
        }

        Collection<Row> result = new ArrayList<>();

        Iterator<Row> iterator = table.iterator();

        while( iterator.hasNext() ){
            Row row = iterator.next();

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

            if(conditionHolds) result.add(row);

        }

        res.rows = result;
        return res;
    }

}
