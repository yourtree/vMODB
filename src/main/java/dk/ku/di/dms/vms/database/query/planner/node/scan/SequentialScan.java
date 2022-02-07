package dk.ku.di.dms.vms.database.query.planner.node.scan;

import dk.ku.di.dms.vms.database.query.planner.node.filter.IFilter;
import dk.ku.di.dms.vms.database.store.refac.Row;
import dk.ku.di.dms.vms.database.store.refac.Table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Callable;

/**
 * aka table scan
 *
 */
public final class SequentialScan implements Callable<Collection<Row>> {

    private final IFilter<Row> filter;

    private final Table table;

    public SequentialScan(final Table table, final IFilter<Row> filter) {
        this.table = table;
        this.filter = filter;
    }

    @Override
    public Collection<Row> call() {

        Collection<Row> result = new ArrayList<>();

        Iterator<Row> iterator = table.iterator();

        while( iterator.hasNext() ){
            Row row = iterator.next();
            if (filter.test(row)) result.add(row);
        }

        return result;
    }

}
