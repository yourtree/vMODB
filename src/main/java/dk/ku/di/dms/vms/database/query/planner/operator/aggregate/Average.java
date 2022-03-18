package dk.ku.di.dms.vms.database.query.planner.operator.aggregate;

import dk.ku.di.dms.vms.database.query.planner.operator.OperatorResult;
import dk.ku.di.dms.vms.database.store.meta.ColumnReference;
import dk.ku.di.dms.vms.database.store.row.Row;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * https://www.tutorialandexample.com/aggregate-functions-in-dbms
 * TODO embrace filters (beneficial in case no joins are previously employed) and having clause
 */
public class Average<T extends Number> implements Supplier<OperatorResult>, Consumer<OperatorResult> {

    private OperatorResult input;

    private final ColumnReference column;

    private final List<ColumnReference> groupByColumns;

    public Average(ColumnReference column, List<ColumnReference> groupByColumns) {
        this.column = column;
        this.groupByColumns = groupByColumns;
    }

    @Override
    public void accept(OperatorResult operatorResult) {
        this.input = operatorResult;
    }

    @Override
    public OperatorResult get() {

        HashMap<Integer, List<Object>> groupedValues = new HashMap<>();

        IntStream intStream = IntStream.of(2,3,4);
//        intStream.filter()
//        intStream.average()

        // having a number of maps equals to the number of groupBy columns can be overwhelming.
        // a better, more general solution to track the dependencies of sets fo values is required

        // the grouping values are hashed
        int sizeOfGroupByClause = groupByColumns.size();

        Object[] valuesForHashing = new Object[sizeOfGroupByClause];
        int i = 0;
        for( final Row row : input.getRows() ){

            for(ColumnReference columnReference : groupByColumns){
                valuesForHashing[i] = row.get(columnReference.columnPosition);
                i++;
            }
            i = 0;

            // hash values
            int hash = Arrays.hashCode( valuesForHashing );
            List<Object> values = groupedValues.get( hash );
            if(groupedValues.get( hash ) == null) {
                values = new ArrayList<>();
                groupedValues.put( hash, values );
            }
            values.add( row.get(column.columnPosition) );

        }

        // TODO finish
        // Stream<T> stream = (Stream<T>) groupedValues.get(1).stream();

        return null;
    }

}
