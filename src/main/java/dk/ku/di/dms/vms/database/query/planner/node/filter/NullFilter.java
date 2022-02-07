package dk.ku.di.dms.vms.database.query.planner.node.filter;

import dk.ku.di.dms.vms.database.store.refac.Row;

public class NullFilter implements IFilter<Row> {

    public final int columnIndex;

    public NullFilter(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    @Override
    public boolean test(Row row) {
        return row.get(columnIndex) == null;
    }
}
