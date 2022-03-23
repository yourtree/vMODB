package dk.ku.di.dms.vms.database.query.planner.tree;

import dk.ku.di.dms.vms.database.query.planner.operator.result.interfaces.IOperatorResult;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class PlanNode {

    // applies only to projections to data classes
    // public Consumer<Object> projector;

    // applies to any execution
    public Supplier<IOperatorResult> supplier;

    // single thread execution
    public Consumer<IOperatorResult> consumer;

    // parallel execution
    public Consumer<CompletableFuture<IOperatorResult>> consumerFuture;
    public BiConsumer<CompletableFuture<IOperatorResult>,CompletableFuture<IOperatorResult>> biConsumerFuture;

    public PlanNode father;
    public PlanNode left;
//    public PlanNode right;
//
//    public boolean isLeaf;

    // https://www.interdb.jp/pg/pgsql03.html

    public PlanNode(Supplier<IOperatorResult> supplier){
        this.supplier = supplier;
    }

    public PlanNode(Consumer<IOperatorResult> consumer){
        this.consumer = consumer;
    }

    public PlanNode(){}

}
