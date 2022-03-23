package dk.ku.di.dms.vms.database.query.planner.operator.constraint;

import dk.ku.di.dms.vms.database.query.planner.operator.result.EntityOperatorResult;
import dk.ku.di.dms.vms.database.query.planner.operator.result.interfaces.IOperatorResult;
import dk.ku.di.dms.vms.database.store.common.IKey;
import dk.ku.di.dms.vms.database.store.index.AbstractIndex;
import dk.ku.di.dms.vms.database.store.table.Table;
import dk.ku.di.dms.vms.infra.AbstractEntity;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class ConstraintEnforcer implements Supplier<IOperatorResult> {

    private final List<? extends AbstractEntity<?>> input;

    private final Map<Table, AbstractIndex<IKey>> indexPerForeignKey;

    private final Table table;

    public ConstraintEnforcer(List<? extends AbstractEntity<?>> input, Map<Table, AbstractIndex<IKey>> indexPerForeignKey, Table table) {
        this.input = input;
        this.indexPerForeignKey = indexPerForeignKey;
        this.table = table;
    }

    @Override
    public EntityOperatorResult get() {

        // naive one by one processing
        for( AbstractEntity<?> entity : this.input ) {

            // for each foreign key, check constraint

            // TODO finish

            // build rows
        }

        // join with the foreign key

        // constraint become a filter

        // TODO in case the constraints are not met, there should be an
        //  observer object that would handle well the end of the procedure

        return new EntityOperatorResult( this.input );
    }
}
