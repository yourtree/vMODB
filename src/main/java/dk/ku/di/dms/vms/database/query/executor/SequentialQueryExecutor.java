package dk.ku.di.dms.vms.database.query.executor;

import dk.ku.di.dms.vms.database.query.planner.OperatorResult;
import dk.ku.di.dms.vms.database.query.planner.PlanNode;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

public class SequentialQueryExecutor implements Supplier<OperatorResult> {

    private final Executor executor;
    private final PlanNode tail;

    public SequentialQueryExecutor(final Executor executor, final PlanNode tail) {
        this.executor = executor;
        this.tail = tail;
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

        return null;

    }

}