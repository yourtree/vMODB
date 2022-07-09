package dk.ku.di.dms.vms.modb.query.planner.operator.projection;

import dk.ku.di.dms.vms.modb.query.analyzer.predicate.GroupByPredicate;
import dk.ku.di.dms.vms.modb.schema.ColumnReference;
import dk.ku.di.dms.vms.modb.query.planner.operator.result.RowOperatorResult;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Default, row-oriented projection
 */
public class RawProjector implements Supplier<RowOperatorResult>, Consumer<RowOperatorResult> {



    public RawProjector(List<ColumnReference> projections, List<GroupByPredicate> groupByPredicates) {

    }


    @Override
    public void accept(RowOperatorResult operatorResult) {

    }

    @Override
    public RowOperatorResult get() {
        return null;
    }
}
