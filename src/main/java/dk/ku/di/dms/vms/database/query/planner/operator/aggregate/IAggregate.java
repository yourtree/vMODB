package dk.ku.di.dms.vms.database.query.planner.operator.aggregate;

import dk.ku.di.dms.vms.database.query.planner.operator.OperatorResult;
import dk.ku.di.dms.vms.database.store.table.Table;

import java.util.function.Consumer;
import java.util.function.Supplier;

public interface IAggregate extends Supplier<OperatorResult>, Consumer<OperatorResult> {

    Table getTable();

}
