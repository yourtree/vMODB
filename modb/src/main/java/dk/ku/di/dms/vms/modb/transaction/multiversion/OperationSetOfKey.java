package dk.ku.di.dms.vms.modb.transaction.multiversion;

import dk.ku.di.dms.vms.modb.common.transaction.TransactionId;
import dk.ku.di.dms.vms.modb.transaction.internal.SingleWriterMultipleReadersFIFO;

/**
 * The set of operations applied to a given index key
 * Maybe the references to DeleteOp and InsertOp are not necessary. Since we have the last write type and the cached entity,
 * they naturally reference the insert (or last updated)...
 */
public class OperationSetOfKey {

    /**
     * Maybe the entry of this map can be a thread local variable?
     * To save the log(n) on the subsequent operations...
     * Contains the write (insert, delete, update) operations of records.
     * If that is a delete, no new records can be added to the key.
     */
    public SingleWriterMultipleReadersFIFO<TransactionId, TransactionWrite> updateHistoryMap;

    /**
     * Nothing impedes the user from deleting and inserting again the same record.
     * A RW/W thread are sequentially spawned, this value always returns correct results.
     * Serves a cache for the last stable write for this key. To avoid traversing all writes performed by TIDs in the history map.
     * It is written during RW/W tasks, so can only be used by on-flight RW/W tasks
     */
    public WriteType lastWriteType;

    /**
     * Only contains inserts and writes.
     * Why this disaggregation? Deletes and inserts
     * for the same key may occur across transactions
     * making it complicated to iterate over previous TIDs and their
     * history entries to find whether there has been a delete
     * It acts as an index for the inserts and deletes made in a key bu different transactions.
     */
    // public final TreeMap<TransactionId, TransactionHistoryEntry> deleteInsertCacheMap;

    /**
     * Entity from the last write operation (insert or update)
     * cached so we can extract the fields changed from the last version
     * to speed up the checking of constraints
     * Only used by on-flight RW/W tasks
     */
    public Object[] cachedEntity;

    public OperationSetOfKey(){
        this.updateHistoryMap = new SingleWriterMultipleReadersFIFO<>();
//        this.updateHistoryMap = (TreeMap<TransactionId, TransactionWrite>)
//                Collections.<TransactionId, TransactionWrite>synchronizedMap( new TreeMap<>() );
//        this.deleteInsertCacheMap = (TreeMap<TransactionId, TransactionHistoryEntry>)
//                Collections.<TransactionId, TransactionHistoryEntry>synchronizedMap( new TreeMap<>() );
    }

}
