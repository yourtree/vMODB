package dk.ku.di.dms.vms.database.query.planner.node.join;

import dk.ku.di.dms.vms.database.query.planner.OperatorResult;
import dk.ku.di.dms.vms.database.store.index.IIndex;
import dk.ku.di.dms.vms.database.store.table.Table;

import java.util.concurrent.Callable;

public class HashJoin implements Callable<OperatorResult> {

    private final Table outerTable;
    private final Table innerTable;

    private final IIndex index;

    public HashJoin(Table outerTable, Table innerTable, IIndex index) {
        this.outerTable = outerTable;
        this.innerTable = innerTable;
        this.index = index;
    }

    @Override
    public OperatorResult call() throws Exception {



        return null;

    }

}
