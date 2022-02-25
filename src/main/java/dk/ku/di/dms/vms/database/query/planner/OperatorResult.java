package dk.ku.di.dms.vms.database.query.planner;

import dk.ku.di.dms.vms.database.store.row.Row;

import java.util.Collection;
import java.util.function.Supplier;

public class OperatorResult implements Supplier<Collection<Row>> {

    // TODO think about spliterator to connect with language processing
    //  (https://docs.oracle.com/javase/8/docs/api/java/util/Spliterator.html)

    public Collection<Row> rows;

    @Override
    public Collection<Row> get() {
        return rows;
    }
}
