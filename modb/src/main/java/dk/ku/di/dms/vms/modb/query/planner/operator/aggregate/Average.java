package dk.ku.di.dms.vms.modb.query.planner.operator.aggregate;

import dk.ku.di.dms.vms.modb.query.planner.operator.result.RowOperatorResult;
import dk.ku.di.dms.vms.modb.store.meta.ColumnReference;
import dk.ku.di.dms.vms.modb.store.row.Row;
import dk.ku.di.dms.vms.modb.store.table.Table;
import dk.ku.di.dms.vms.store.meta.ColumnReference;
import dk.ku.di.dms.vms.store.row.Row;
import dk.ku.di.dms.vms.store.table.Table;
import dk.ku.di.dms.vms.utils.Pair;

import java.util.*;
import java.util.stream.DoubleStream;

import static java.util.stream.DoubleStream.Builder;

/**
 * https://www.tutorialandexample.com/aggregate-functions-in-dbms
 * TODO embrace filters (beneficial in case no joins are previously employed) and having clause
 */
public class Average implements IAggregate {

    private RowOperatorResult input;

    private final ColumnReference column;

    private final List<ColumnReference> groupByColumns;

    public Average(ColumnReference column, List<ColumnReference> groupByColumns) {
        this.column = column;
        this.groupByColumns = groupByColumns;
    }

    @Override
    public void accept(RowOperatorResult operatorResult) {
        this.input = operatorResult;
    }

    @Override
    public RowOperatorResult get() {

        HashMap<Integer, Builder> valuesForAggregation = new HashMap<>();
        HashMap<Integer, Object[]> columnsForAggregation = new HashMap<>();

        // having a number of maps equals to the number of groupBy columns can be overwhelming.
        // a better, more general solution to track the dependencies of sets fo values is required

        for( final Row row : input.getRows() ){
            Pair<Integer,Object[]> pair = getGroupIdentifier(row);
            if(valuesForAggregation.get( pair.getFirst() ) == null) {
                Builder stream = DoubleStream.builder();
                valuesForAggregation.put( pair.getFirst(), stream );
                columnsForAggregation.put( pair.getFirst(), pair.getSecond() );
            }
            // TODO check whether there is a better strategy, e.g., https://stackoverflow.com/questions/34446626/java-using-streams-on-abstract-datatypes
            valuesForAggregation.get( pair.getFirst() ).add( ((Number) row.get(column.columnPosition)).doubleValue() );

        }

        List<Row> output = new ArrayList<>( valuesForAggregation.size() );

        // iterate through each group of values
        for(Map.Entry<Integer, Builder> entry : valuesForAggregation.entrySet()){

            // build stream
            DoubleStream stream = entry.getValue().build();

            // compute avg
            OptionalDouble optionalDouble = stream.average();
            double avg = optionalDouble.isPresent() ? optionalDouble.getAsDouble() : 0; // TODO log

            //store in operator result
            Object[] rowOutput = columnsForAggregation.get(entry.getKey());
            rowOutput[columnsForAggregation.size()] = avg;
            Row row = new Row( rowOutput );

            output.add( row );

        }

        return new RowOperatorResult( output );
    }

    private Pair<Integer,Object[]> getGroupIdentifier(Row row){

        if(groupByColumns != null) {

            Object[] valuesForHashing = new Object[groupByColumns.size() + 1];

            int i = 0;
            for (ColumnReference columnReference : this.groupByColumns) {
                valuesForHashing[i] = row.get(columnReference.columnPosition);
                i++;
            }

            // hash values
            int hash = Arrays.hashCode(valuesForHashing);

            return new Pair<>( hash, valuesForHashing );

        }

        Object[] rowOutput = new Object[2];
        rowOutput[0] = 1; // can be anything...
        return new Pair<>( 1, rowOutput );

    }


    @Override
    public Table getTable() {
        return this.column.table;
    }
}
