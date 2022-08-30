package dk.ku.di.dms.vms.modb.query.complex.executor;

import dk.ku.di.dms.vms.modb.query.planner.operator.result.interfaces.IOperatorResult;
import dk.ku.di.dms.vms.modb.query.complex.planner.tree.PlanNode;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ConcurrentQueryExecutor implements Supplier<CompletableFuture<IOperatorResult>> {

    private final Executor executor;

    private final PlanNode head;

    private final Map<PlanNode, PlanNode[]> predecessorLeafNodes;

    public ConcurrentQueryExecutor(final Executor executor,
                                   final PlanNode head,
                                   final Map<PlanNode, PlanNode[]> predecessorLeafNodes) {
        this.executor = executor;
        this.head = head;
        this.predecessorLeafNodes = predecessorLeafNodes;
    }

    /**
     * while there are remaining tasks, continue in a loop
     * to schedule the tasks when their dependencies have been
     * fulfilled
     */
    @Override
    public CompletableFuture<IOperatorResult> get() {

        CompletableFuture<IOperatorResult> finalTask = null;

        // while we still have a predecessor to schedule
        while (predecessorLeafNodes.size() > 0) {

            for (Map.Entry<PlanNode, PlanNode[]> entry : this.predecessorLeafNodes.entrySet()) {

                PlanNode[] childrenList = entry.getValue();
                PlanNode iNode = entry.getKey();

                if (childrenList.length == 2) {
                    // two children always lead to a bi consumer

                    BiConsumer<CompletableFuture<IOperatorResult>, CompletableFuture<IOperatorResult>> node =
                            iNode.biConsumerFuture;
                    PlanNode left = childrenList[0];
                    PlanNode right = childrenList[1];

                    // TODO make sure the push order is guaranteed by the executor
                    // concurrent queue should guarantee the order of queuing
                    CompletableFuture<IOperatorResult> op1 =
                            CompletableFuture.supplyAsync(left.supplier,executor);
                    CompletableFuture<IOperatorResult> op2 =
                            CompletableFuture.supplyAsync(right.supplier,executor);

                    // avoid blocking now, so we can continue building the execution tree
                    node.accept(op1, op2);

                    if (predecessorLeafNodes.get(iNode.father) == null) {
                        PlanNode[] newChildrenList = new PlanNode[2];
                        newChildrenList[0] = iNode;
                        predecessorLeafNodes.put(iNode.father, newChildrenList);
                    } else {
                        PlanNode[] _children = predecessorLeafNodes.get(iNode.father);
                        _children[1] = iNode;
                    }

                } else if (childrenList.length == 1) { // == 1

                    Consumer<CompletableFuture<IOperatorResult>> node = iNode.consumerFuture;

                    PlanNode children = childrenList[0];

                    CompletableFuture<IOperatorResult> op =
                            CompletableFuture.supplyAsync(children.supplier,executor);

                    node.accept(op);

                    PlanNode[] newChildrenList = new PlanNode[1];
                    newChildrenList[0] = iNode;
                    predecessorLeafNodes.put(iNode.father, newChildrenList);
                } else {
                    // TODO or equals to head?
                    finalTask = CompletableFuture.supplyAsync(iNode.supplier);
                }

            }
        }

        return finalTask;

    }

}