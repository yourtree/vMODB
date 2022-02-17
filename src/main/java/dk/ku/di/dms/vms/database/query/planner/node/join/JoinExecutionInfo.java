package dk.ku.di.dms.vms.database.query.planner.node.join;

public class JoinExecutionInfo {

    // selectivity of an index = number of rows with that value / total number of rows

    // selectivity of the index chosen. the lower, the better
    public double leftSelectivity;

    public double rightSelectivity;

}
