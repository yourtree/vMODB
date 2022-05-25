package dk.ku.di.dms.vms.modb.query.planner.operator.result;

import dk.ku.di.dms.vms.modb.query.planner.operator.result.interfaces.IReturnableOperatorResult;
import dk.ku.di.dms.vms.modb.common.meta.Row;

import java.util.ArrayList;
import java.util.Collection;

public class RowOperatorResult implements IReturnableOperatorResult<Row> {

    // TODO think about spliterator to connect with language processing
    //  (https://docs.oracle.com/javase/8/docs/api/java/util/Spliterator.html)

    // The collection is because the internal data structures require it
    private final Collection<Row> rows;

    public RowOperatorResult(final Collection<Row> rows){
        this.rows = rows;
    }

    public RowOperatorResult(){
        this.rows = new ArrayList<>();
    }

    public RowOperatorResult(int size){
        this.rows = new ArrayList<>(size);
    }

    public Collection<Row> getRows() {
        return rows;
    }

    @Override
    public void accept(Row row) {
        this.rows.add(row);
    }

}
