package dk.ku.di.dms.vms.database.query.planner.node.join;

import dk.ku.di.dms.vms.database.query.planner.OperatorResult;
import dk.ku.di.dms.vms.database.query.planner.node.filter.FilterInfo;
import dk.ku.di.dms.vms.database.query.planner.node.filter.IFilter;
import dk.ku.di.dms.vms.database.query.planner.utils.IdentifiableNode;
import dk.ku.di.dms.vms.database.store.index.HashIndex;
import dk.ku.di.dms.vms.database.store.row.IKey;
import dk.ku.di.dms.vms.database.store.row.Row;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A hash join where its dependencies are fulfilled from the start
 * In other words, previous scanning and transformation steps are not necessary
 */

public class HashJoin implements Supplier<OperatorResult>, IJoin
{

    private final HashIndex innerIndex;
    private final HashIndex outerIndex;

    private final FilterInfo filterInner;
    private final FilterInfo filterOuter;

    public HashJoin(final HashIndex innerIndex,
                    final HashIndex outerIndex,
                    final FilterInfo filterInner,
                    final FilterInfo filterOuter) {
        this.innerIndex = innerIndex;
        this.outerIndex = outerIndex;
        this.filterInner = filterInner;
        this.filterOuter = filterOuter;
    }

    @Override
    public OperatorResult get() {

        for(final Map.Entry<IKey,Row> rowEntry : innerIndex.entrySet()){

            IKey currRowKey = rowEntry.getKey();
            Row currRowVal = rowEntry.getValue();

            // check filter first
            boolean leftIsGreen = true;

            if(filterInner.filters != null){
                leftIsGreen = check(
                        currRowVal,
                        filterInner.filters,
                        filterInner.filterColumns,
                        filterInner.filterParams);
            }

            Row probedRow = null;
            // and then probe
            if(leftIsGreen && outerIndex.retrieve(currRowKey, probedRow)){
                boolean rightIsGreen = true;
                // then check whether the probed row satisfy
                if(filterOuter.filters != null) {
                    rightIsGreen = check(
                            currRowVal,
                            filterInner.filters,
                            filterInner.filterColumns,
                            filterInner.filterParams);
                }

                if(rightIsGreen){
                    // TODO push result upstream... it would call the accept API of a consumer
                }

            }

        }

        // TODO finish

        return null;

    }

    private boolean check(final Row row,
                          final IFilter[] filters,
                          final int[] filterColumns,
                          final Collection<IdentifiableNode<Object>> filterParams){

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
