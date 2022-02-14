package dk.ku.di.dms.vms.database.query.planner;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class PlanNode {

    public final Supplier<OperatorResult> supplier;
    public final Consumer<OperatorResult> consumer;
    public final BiConsumer<OperatorResult,OperatorResult> biConsumer;

    public final PlanNode father;
    public final PlanNode left;
    public final PlanNode right;

    public final boolean isLeaf;

    // https://www.interdb.jp/pg/pgsql03.html

    public PlanNode(Supplier<OperatorResult> supplier,
                    Consumer<OperatorResult> consumer,
                    BiConsumer<OperatorResult, OperatorResult> biConsumer,
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
