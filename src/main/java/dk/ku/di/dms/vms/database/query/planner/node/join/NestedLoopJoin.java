package dk.ku.di.dms.vms.database.query.planner.node.join;

public class NestedLoopJoin {

    private JoinCondition filterJoin;

    public NestedLoopJoin(){}

    public NestedLoopJoin(JoinCondition filterJoin) {
        this.filterJoin = filterJoin;
    }
}
