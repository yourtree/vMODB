package dk.ku.di.dms.vms.modb.query.complex.planner.tree;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class PlanNode {

    // applies only to projections to data classes
    // public Consumer<Object> projector;

    // applies to any execution
    public Supplier<?> supplier;

    // single thread execution
    public Consumer<?> consumer;

    // parallel execution
    public Consumer<CompletableFuture<?>> consumerFuture;
    public BiConsumer<CompletableFuture<?>,CompletableFuture<?>> biConsumerFuture;

    public PlanNode father;
    public PlanNode left;
//    public PlanNode right;
//
//    public boolean isLeaf;

    // https://www.interdb.jp/pg/pgsql03.html

    public PlanNode(Supplier<?> supplier){
        this.supplier = supplier;
    }

    public PlanNode(Consumer<?> consumer){
        this.consumer = consumer;
    }

    public PlanNode(){}

}
