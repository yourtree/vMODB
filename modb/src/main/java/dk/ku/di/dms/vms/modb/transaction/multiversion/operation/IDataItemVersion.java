package dk.ku.di.dms.vms.modb.transaction.multiversion.operation;

/**
 * Delete operation carries no payload, so no need to return it
 */
public interface IDataItemVersion {

    Operation operation();

    default UpdateOp asUpdate(){
        throw new IllegalStateException("Not an update node.");
    }

    default InsertOp asInsert(){
        throw new IllegalStateException("Not an insert node.");
    }

}
