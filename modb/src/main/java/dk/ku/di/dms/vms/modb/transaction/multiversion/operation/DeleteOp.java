package dk.ku.di.dms.vms.modb.transaction.multiversion.operation;

public class DeleteOp extends DataItemVersion {

    protected DeleteOp(long tid) {
        super(tid);
    }

    @Override
    public Operation operation() {
        return Operation.DELETE;
    }

    @Override
    public DeleteOp asDelete() {
        return this;
    }
}
