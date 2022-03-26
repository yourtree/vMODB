package dk.ku.di.dms.vms.modb.query.planner.operator.aggregate;

import dk.ku.di.dms.vms.modb.query.planner.operator.result.interfaces.IOperatorResult;
import dk.ku.di.dms.vms.modb.store.table.Table;
import dk.ku.di.dms.vms.modb.query.planner.operator.result.RowOperatorResult;

import java.util.function.Consumer;
import java.util.function.Supplier;

public interface IAggregate extends Supplier<IOperatorResult>, Consumer<RowOperatorResult> {

    Table getTable();

}
