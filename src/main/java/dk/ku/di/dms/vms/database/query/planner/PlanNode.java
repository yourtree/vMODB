package dk.ku.di.dms.vms.database.query.planner;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class PlanNode {

    public final Supplier<OperatorResult> supplier;
    public final Consumer<CompletableFuture<OperatorResult>> consumer;
    public final BiConsumer<CompletableFuture<OperatorResult>,CompletableFuture<OperatorResult>> biConsumer;

    public final PlanNode father;
    public final PlanNode left;
    public final PlanNode right;

    public final boolean isLeaf;

    // https://www.interdb.jp/pg/pgsql03.html

    public PlanNode(Supplier<OperatorResult> supplier,
                    Consumer<CompletableFuture<OperatorResult>> consumer,
                    BiConsumer<CompletableFuture<OperatorResult>,CompletableFuture<OperatorResult>> biConsumer,
                    PlanNode father,
                    PlanNode left,
                    PlanNode right,
                    boolean isLeaf) {
        this.supplier = supplier;
        this.consumer = consumer;
        this.biConsumer = biConsumer;
        this.father = father;
        this.left = left;
        this.right = right;
        this.isLeaf = isLeaf;
    }
}
