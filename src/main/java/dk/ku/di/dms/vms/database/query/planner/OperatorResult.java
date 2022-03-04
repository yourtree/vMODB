package dk.ku.di.dms.vms.database.query.planner;

import dk.ku.di.dms.vms.database.store.row.Row;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class OperatorResult implements Supplier<Collection<Row>>, Consumer<Row> {

    // TODO think about spliterator to connect with language processing
    //  (https://docs.oracle.com/javase/8/docs/api/java/util/Spliterator.html)

    private final Collection<Row> rows;

    public OperatorResult(final Collection<Row> rows){
        this.rows = rows;
    }

    public OperatorResult(){
        this.rows = new ArrayList<>();
    }

    public OperatorResult(int size){
        this.rows = new ArrayList<>(size);
    }

    @Override
    public Collection<Row> get() {
        return rows;
    }

    @Override
    public void accept(Row row) {
        this.rows.add(row);
    }
}
