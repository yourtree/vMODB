package dk.ku.di.dms.vms.modb.query.planner.operator.constraint;

import dk.ku.di.dms.vms.modb.common.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.query.planner.operator.result.EntityOperatorResult;
import dk.ku.di.dms.vms.modb.query.planner.operator.result.interfaces.IOperatorResult;
import dk.ku.di.dms.vms.modb.query.planner.operator.result.VoidOperatorResult;
import dk.ku.di.dms.vms.modb.query.planner.operator.result.meta.Status;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class BulkInsert implements Consumer<IOperatorResult>, Supplier<VoidOperatorResult> {

    private EntityOperatorResult input;

    @Override
    public void accept(IOperatorResult operatorResult) {
        this.input = operatorResult.asEntityOperatorResult();
    }

    @Override
    public VoidOperatorResult get() {

        // naive one by one processing
        for( IEntity<?> entity : this.input.getEntities() ) {

            // for each foreign key, check constraint

            // TODO finish

            // build rows
        }

        // join with the foreign key

        // constraint become a filter

        // TODO in case the constraints are not met, there should be an
        //  observer object that would handle well the end of the procedure

        return new VoidOperatorResult( Status.SUCCESS );
    }

}
