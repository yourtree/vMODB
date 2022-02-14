package dk.ku.di.dms.vms.database.query.executor;

import dk.ku.di.dms.vms.database.query.planner.OperatorResult;
import dk.ku.di.dms.vms.database.query.planner.PlanNode;
import dk.ku.di.dms.vms.database.query.planner.utils.IdentifiableNode;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class QueryExecutor implements Supplier<OperatorResult> {

    private final Executor executor;

    private final PlanNode head;

    private final Map<IdentifiableNode<PlanNode>,List<PlanNode>> predecessorLeafNodes;

    public QueryExecutor(final Executor executor,
                         final PlanNode head,
                         final Map<IdentifiableNode<PlanNode>,List<PlanNode>> predecessorLeafNodes) {
        this.executor = executor;
        this.head = head;
        this.predecessorLeafNodes = predecessorLeafNodes;
    }

    /** while there are remaining tasks, continue in a loop
        to schedule the tasks when their dependencies have been
        fulfilled
    */
    @Override
    public OperatorResult get() {

        // boolean availableTasks = !predecessorLeafNodes.isEmpty();

        // TODO the leaves of a node always form a pair?

        for( Map.Entry<IdentifiableNode<PlanNode>,List<PlanNode>> entry : this.predecessorLeafNodes.entrySet() ){

            IdentifiableNode<PlanNode> iNode = entry.getKey();
            BiConsumer<OperatorResult,OperatorResult> node = iNode.object.biConsumer;
            PlanNode left = entry.getValue().get(0);
            PlanNode right = entry.getValue().get(1);
            CompletableFuture<OperatorResult> op1 =
                    CompletableFuture.supplyAsync( left.supplier );
            CompletableFuture<OperatorResult> op2 =
                    CompletableFuture.supplyAsync( right.supplier );

            node.accept( op1.join(), op2.join() );

//            CompletableFuture<OperatorResult> op3 = future
//                    .biAccept( op1, op2, iNode.object.biConsumer, null );

            // CompletableFuture<Void> future = op1.thenCombine( op2,  )
            //thenAcceptAsync( iNode.object.biConsumer.accept(  ); )
                            //.thenCombineAsync( right.supplier, ( l,r ) -> iNode.object.biConsumer.accept(l, (OperatorResult) r) );
//                            .thenCombineAsync(
//                                    right.supplier, (l,r) -> iNode.object.biConsumer.accept(l,r) );

        }

        // CompletableFuture<OperatorResult>

        return null;
    }
}
