package dk.ku.di.dms.vms.database.query.planner.node.join;

public class NestedLoopJoin {

    final FilterJoinInfo filterJoin;

    public NestedLoopJoin(FilterJoinInfo filterJoin) {
        this.filterJoin = filterJoin;
    }
}
