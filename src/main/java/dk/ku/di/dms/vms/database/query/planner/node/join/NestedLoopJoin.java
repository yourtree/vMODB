package dk.ku.di.dms.vms.database.query.planner.node.join;

public class NestedLoopJoin {

    final JoinCondition filterJoin;

    public NestedLoopJoin(JoinCondition filterJoin) {
        this.filterJoin = filterJoin;
    }
}
