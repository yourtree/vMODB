package dk.ku.di.dms.vms.database.query.executor;

import dk.ku.di.dms.vms.database.query.planner.OperatorResult;
import dk.ku.di.dms.vms.database.query.planner.PlanNode;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class SequentialQueryExecutor implements Supplier<OperatorResult> {

    private final PlanNode head;

    private final Map<PlanNode, PlanNode[]> predecessorLeafNodes;

    public SequentialQueryExecutor(final PlanNode head,
                                   final Map<PlanNode, PlanNode[]> predecessorLeafNodes) {
        this.head = head;
        this.predecessorLeafNodes = predecessorLeafNodes;
    }

    /**
     * while there are remaining tasks, continue in a loop
     * to schedule the tasks when their dependencies have been
     * fulfilled
     */
    @Override
    public OperatorResult get() {

        CompletableFuture<OperatorResult> finalTask = null;

        // TODO define the best strategy

        // single thread strategy

        // heuristic based

        // parallel strategy

        // while we still have a predecessor to schedule
        while (predecessorLeafNodes.size() > 0) {

            for (Map.Entry<PlanNode, PlanNode[]> entry : this.predecessorLeafNodes.entrySet()) {



            }
        }

        return null;

    }

}