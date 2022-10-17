package dk.ku.di.dms.vms.modb.transaction.multiversion;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IIndexKey;

/**
 * Contains:
 * - the transaction that have been modified a data item
 * - the task that have modified the data item
 * - the index
 * - the key (in the index) corresponding to the data item
 * - the change/value (after-write): specific for the type of write
 *
 */
public class DataItemVersion {

    // transaction id that modified the item. don't need it since it is stored inside the map
    // private TransactionId transactionId;

    public IKey pk;

    public IIndexKey indexKey;

    public TransactionWrite entry;

    // previous not necessary anymore since I have all the object array
    // public DataItemVersion previous;

    public DataItemVersion(//TransactionId transactionId,
                           IIndexKey indexKey, IKey pk, TransactionWrite entry) {
        //this.transactionId = transactionId;
        this.indexKey = indexKey;
        this.pk = pk;
        this.entry = entry;
    }

//    public TransactionId tid(){
//        return this.transactionId;
//    }

    public IKey pk() {
        return this.pk;
    }

    public IIndexKey indexKey() {
        return this.indexKey;
    }

//    public static UpdateOp newVersionUpdate(TransactionId tid, Object[] values, IIndexKey indexKey, IKey pk){
//        return new UpdateOp(tid, values, indexKey, pk);
//    }
//
//    public static InsertOp newVersionInsert(TransactionId tid, Object[] values, IIndexKey indexKey, IKey pk){
//        return new InsertOp(tid, values, indexKey, pk);
//    }
//
//    public static DeleteOp newVersionDelete(TransactionId tid, IIndexKey indexKey, IKey pk){
//        return new DeleteOp(tid, indexKey, pk);
//    }

}
