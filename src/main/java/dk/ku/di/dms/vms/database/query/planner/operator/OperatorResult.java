package dk.ku.di.dms.vms.database.query.planner.operator;

import dk.ku.di.dms.vms.database.store.row.Row;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class OperatorResult implements //Supplier<Collection<Row>>,
        Consumer<Row> {

    // TODO think about spliterator to connect with language processing
    //  (https://docs.oracle.com/javase/8/docs/api/java/util/Spliterator.html)

    // The collection is because the internal data structures require it
    private Collection<Row> rows;

    // As we are building the DTOs, we can opt for list
    // Maybe all DTOs must have the same inheritance, e.g., AbstractDTO
    private List<Object> dataTransferObjects;

    public OperatorResult(final Collection<Row> rows){
        this.rows = rows;
    }

    public OperatorResult(){
        this.rows = new ArrayList<>();
    }

    public OperatorResult(int size){
        this.rows = new ArrayList<>(size);
    }
    public OperatorResult(final List<Object> dataTransferObjects){
        this.dataTransferObjects = dataTransferObjects;
    }

    public Collection<Row> getRows() {
        return rows;
    }

    public List<Object> getDataTransferObjects() {
        return dataTransferObjects;
    }

    @Override
    public void accept(Row row) {
        this.rows.add(row);
    }
}
