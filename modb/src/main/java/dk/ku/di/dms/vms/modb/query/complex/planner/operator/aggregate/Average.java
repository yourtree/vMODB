package dk.ku.di.dms.vms.modb.query.complex.planner.operator.aggregate;

import dk.ku.di.dms.vms.modb.definition.ColumnReference;
import dk.ku.di.dms.vms.modb.definition.Row;
import dk.ku.di.dms.vms.modb.query.complex.planner.operator.AbstractOperator;

import java.util.*;
import java.util.stream.DoubleStream;

import static java.util.stream.DoubleStream.Builder;

/**
 * https://www.tutorialandexample.com/aggregate-functions-in-dbms
 *
 * Two types (MMDB):
 * - also performs the scan
 * - receives result from downstream operator (e.g., join)
 * - - In this case, it is necessary to have a descriptor of the join output
 * - - Let's just assume that this descriptor is fixed,
 *      i.e., it can be assumed it does not change for now
 *
 *  Another appropriate optimization is to materialize the
 *  projection through the aggregate operation. Otherwise a duplicate cost would incur
 *  for the upstream (projection) operator. Why? The aggregate would need to include
 *  the address of the tuples in each respective group result. Thus, the projection would
 *  again have to read the same addresses.
 *
 *  Maybe the same idea can be applied to Scan and Join?
 *
 */
public class Average extends AbstractOperator {

//        SequenceLayout SEQUENCE_LAYOUT = MemoryLayout.sequenceLayout(1024, ValueLayout.JAVA_INT);
//        MemorySegment segment = MemorySegment.allocateNative(SEQUENCE_LAYOUT, scope);
//
//        int sum = segment.elements(ValueLayout.JAVA_INT).parallel()
//                                            .mapToInt(s -> s.get(ValueLayout.JAVA_INT, 0))
//                                            .sum();


    private final ColumnReference column;

    private final List<ColumnReference> groupByColumns;

    public Average(int id, ColumnReference column, List<ColumnReference> groupByColumns) {
        super(id, 0);
        this.column = column;
        this.groupByColumns = groupByColumns;
    }


    public Object get() {

        HashMap<Integer, Builder> valuesForAggregation = new HashMap<>();
        HashMap<Integer, Object[]> columnsForAggregation = new HashMap<>();

        // having a number of maps equals to the number of groupBy columns can be overwhelming.
        // a better, more general solution to track the dependencies of sets fo values is required

//        for( Row row : input.getRows() ){
//            GroupIdentifier group = getGroupWhereRowBelongsTo(row);
//            if(valuesForAggregation.get( group.hash() ) == null) {
//                Builder stream = DoubleStream.builder();
//                valuesForAggregation.put( group.hash(), stream );
//                columnsForAggregation.put( group.hash(), group.valuesForHashing() );
//            }
//            // TODO check whether there is a better strategy, e.g., https://stackoverflow.com/questions/34446626/java-using-streams-on-abstract-datatypes
//            valuesForAggregation.get( group.hash() ).add( ((Number) row.get(column.columnPosition)).doubleValue() );
//
//        }

        List<Row> output = new ArrayList<>( valuesForAggregation.size() );

        // iterate through each group of values
        for(Map.Entry<Integer, Builder> entry : valuesForAggregation.entrySet()){

            // build stream
            DoubleStream stream = entry.getValue().build();

            // compute avg
            OptionalDouble optionalDouble = stream.average();
            double avg = optionalDouble.isPresent() ? optionalDouble.getAsDouble() : 0;

            //store in operator result
            Object[] rowOutput = columnsForAggregation.get(entry.getKey());
            rowOutput[columnsForAggregation.size()] = avg;
            Row row = new Row( rowOutput );

            output.add( row );

        }

        return null;
    }

    private record GroupIdentifier(
        int hash,
        Object[] valuesForHashing)
        {};

    private GroupIdentifier getGroupWhereRowBelongsTo(Row row){

        if(groupByColumns != null) {

            Object[] valuesForHashing = new Object[groupByColumns.size() + 1];

            int i = 0;
            for (ColumnReference columnReference : this.groupByColumns) {
                valuesForHashing[i] = row.get(columnReference.columnPosition);
                i++;
            }

            // hash values
            int hash = Arrays.hashCode(valuesForHashing);

            return new GroupIdentifier( hash, valuesForHashing );

        }

        Object[] rowOutput = new Object[2];
        rowOutput[0] = -1; // can be anything...
        return new GroupIdentifier( -1, rowOutput );

    }


}
