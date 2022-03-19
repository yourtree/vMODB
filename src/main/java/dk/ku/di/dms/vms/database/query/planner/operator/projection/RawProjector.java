package dk.ku.di.dms.vms.database.query.planner.operator.projection;

import dk.ku.di.dms.vms.database.query.analyzer.predicate.GroupByPredicate;
import dk.ku.di.dms.vms.database.query.planner.operator.OperatorResult;
import dk.ku.di.dms.vms.database.store.meta.ColumnReference;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Default, row-oriented projection
 */
public class RawProjector implements Supplier<OperatorResult>, Consumer<OperatorResult> {



    public RawProjector(List<ColumnReference> projections, List<GroupByPredicate> groupByPredicates) {

    }


    @Override
    public void accept(OperatorResult operatorResult) {

    }

    @Override
    public OperatorResult get() {
        return null;
    }
}
