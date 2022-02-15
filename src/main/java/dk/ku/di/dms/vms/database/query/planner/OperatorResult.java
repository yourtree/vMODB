package dk.ku.di.dms.vms.database.query.planner;

import dk.ku.di.dms.vms.database.store.row.Row;

import java.util.Collection;
import java.util.function.Supplier;

public class OperatorResult implements Supplier<Collection<Row>> {

    // TODO think about splitterator to connect with language processing

    public Collection<Row> rows;

    @Override
    public Collection<Row> get() {
        return rows;
    }
}
