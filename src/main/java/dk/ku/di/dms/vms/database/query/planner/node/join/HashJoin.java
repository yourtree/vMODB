package dk.ku.di.dms.vms.database.query.planner.node.join;

import dk.ku.di.dms.vms.database.query.planner.OperatorResult;
import dk.ku.di.dms.vms.database.store.index.IIndex;
import dk.ku.di.dms.vms.database.store.table.Table;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * A hash join where its dependencies are fulfilled from the start
 * In other words, so previous scanning and transformation steps are necessary
 */

public class HashJoin implements Supplier<OperatorResult>
{

    private final Table innerTable;
    private final Table outerTable;

    private final IIndex index;

    public HashJoin(Table innerTable, Table outerTable, IIndex index) {
        this.innerTable = innerTable;
        this.outerTable = outerTable;
        this.index = index;
    }

    @Override
    public OperatorResult get() {

        // TODO finish

        return null;

    }

}
