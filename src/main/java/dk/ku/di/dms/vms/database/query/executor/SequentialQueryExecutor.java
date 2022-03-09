package dk.ku.di.dms.vms.database.query.executor;

import dk.ku.di.dms.vms.database.query.planner.OperatorResult;
import dk.ku.di.dms.vms.database.query.planner.PlanNode;

import java.util.function.Supplier;

public class SequentialQueryExecutor implements Supplier<OperatorResult> {

    private PlanNode node;

    public SequentialQueryExecutor(final PlanNode node) {
        this.node = node;
    }

    /**
     * while there are remaining tasks, continue in a loop
     * to schedule the tasks when their dependencies have been
     * fulfilled
     */
    @Override
    public OperatorResult get() {

        OperatorResult result = null;

        // while we still have a node to schedule
        while(true){

            // with an executor it would be like that
            // Future<OperatorResult> futureResult = this.executor.submit( () -> tail.supplier.get() );

            result = node.supplier.get();

            node = node.father;

            if(node == null) break;

            if(node.consumer != null) {
                node.consumer.accept(result);
            }
        }

        return result;

    }

}