package dk.ku.di.dms.vms.modb.transaction.multiversion;

import dk.ku.di.dms.vms.modb.transaction.multiversion.operation.DataItemVersion;

public class OperationNode {

    public final DataItemVersion operation;

    public final OperationNode next;

    public OperationNode(DataItemVersion operation, OperationNode next) {
        this.operation = operation;
        this.next = next;
    }

}
