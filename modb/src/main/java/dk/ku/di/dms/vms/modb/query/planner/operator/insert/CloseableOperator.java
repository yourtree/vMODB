package dk.ku.di.dms.vms.modb.query.planner.operator.insert;

public interface CloseableOperator {

    void signalEndOfStream();

}
