package dk.ku.di.dms.vms.database.query.planner.node.scan;

import dk.ku.di.dms.vms.database.query.planner.node.filter.IFilter;
import dk.ku.di.dms.vms.database.store.meta.DataType;
import dk.ku.di.dms.vms.database.store.row.Row;
import dk.ku.di.dms.vms.database.store.table.Table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Callable;

/**
 * aka table scan
 * A thread should run the scan
 */
public final class SequentialScan implements Callable<Collection<Row>> {

    /**
     * The table to iterate with
     */
    private final Table table;

    /**
     * Filters are agnostic to the columns they are accessing
     * so they can be reused more easily
     */
    private final IFilter[] filters;

    /**
     * The scan operator requires the column to be accessed for each filter
     */
    private final int[] filterColumns;

    /**
     * The data types of the filters involde in this scan
     */
    private final DataType[] filterDataTypes;

    public SequentialScan(final Table table,
                          final IFilter[] filters,
                          final int[] filterColumns,
                          final DataType[] filterDataTypes) {
        this.table = table;
        this.filters = filters;
        this.filterColumns = filterColumns;
        this.filterDataTypes = filterDataTypes;
    }

    /**
     * No filter scan. For instance, SELECT * FROM table
     * @param table
     */
    public SequentialScan(final Table table) {
        this.table = table;
        this.filters = null;
        this.filterColumns = null;
        this.filterDataTypes = null;
    }

    @Override
    public Collection<Row> call() {

        final boolean noFilter = filters == null;

        if (noFilter) return table.rows();

        Collection<Row> result = new ArrayList<>();

        Iterator<Row> iterator = table.iterator();

        while( iterator.hasNext() ){
            Row row = iterator.next();

            boolean conditionHolds = true;
            int filterIdx = 0;
            IFilter currFilter;
            while( conditionHolds && filterIdx < filters.length ){
                currFilter = filters[filterIdx];
                switch (filterDataTypes[filterIdx]){
                    case INT: conditionHolds = currFilter.test( row.getInt( filterColumns[filterIdx] ) ); break;
                    case STRING: conditionHolds = currFilter.test( row.getString( filterColumns[filterIdx] ) ); break;
                    case DOUBLE: conditionHolds = currFilter.test( row.getDouble( filterColumns[filterIdx] ) ); break;
                    case LONG: conditionHolds = currFilter.test( row.getLong( filterColumns[filterIdx] ) ); break;
                    case CHAR: conditionHolds = currFilter.test( row.getCharacter( filterColumns[filterIdx] ) ); break;
                }

                // no need to continue anymore
                if(!conditionHolds) break;

                filterIdx++;

            }

            if(conditionHolds) result.add(row);

        }

        return result;
    }

}
