package dk.ku.di.dms.vms.modb.query.planner.operator.scan;

import dk.ku.di.dms.vms.modb.query.planner.operator.filter.FilterInfo;
import dk.ku.di.dms.vms.modb.query.planner.operator.filter.IFilter;
import dk.ku.di.dms.vms.modb.query.planner.operator.result.interfaces.IOperatorResult;
import dk.ku.di.dms.vms.modb.common.utils.IdentifiableNode;
import dk.ku.di.dms.vms.modb.store.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.store.common.IKey;
import dk.ku.di.dms.vms.modb.store.row.Row;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Supplier;

public abstract class AbstractScan implements Supplier<IOperatorResult> {

    /**
     * The index to iterate with
     */
    protected final AbstractIndex<IKey> index;

    /**
     * Filters are agnostic to the columns they are accessing,
     * so they can be reused more easily
     */
    protected final IFilter<?>[] filters;

    /**
     * The scan operator requires the column to be accessed for each filter
     */
    protected final int[] filterColumns;

    /** The parameters of the filters */
    protected Collection<IdentifiableNode<Object>> filterParams;

    public AbstractScan(final AbstractIndex<IKey> index,
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
    public AbstractScan(final AbstractIndex<IKey> index) {
        this.index = index;
        this.filters = null;
        this.filterColumns = null;
        this.filterParams = null;
    }

    @SuppressWarnings({"unchecked","rawtypes"})
    protected boolean check(final Row row){

        boolean conditionHolds = true;
        int filterIdx = 0;

        IFilter currFilter;

        Iterator<IdentifiableNode<Object>> paramsIterator = filterParams.iterator();
        IdentifiableNode<Object> currParam = null;
        if (paramsIterator.hasNext()){
            currParam = paramsIterator.next();
        }

        while( conditionHolds && filterIdx < filters.length ){
            currFilter = filters[filterIdx];

            // unchecked cast, but we know it is safe since the analyzer makes sure that
            if(currParam != null && currParam.id() == filterIdx) {

//                FilterTestAnother filterTestAnother = new FilterTestAnother((Comparator<Integer>) Integer::compareTo);
//                filterTestAnother.eval( row.get(filterColumns[filterIdx]) );

                conditionHolds = currFilter.eval( row.get(filterColumns[filterIdx]), currParam );
                if (paramsIterator.hasNext()){
                    currParam = paramsIterator.next();
                } else {
                    currParam = null;
                }
            }
            else {
                conditionHolds = currFilter.eval( row.get(filterColumns[filterIdx]) );
            }

            // no need to continue anymore
            if(!conditionHolds) break;

            filterIdx++;

        }

        return conditionHolds;

    }

}
