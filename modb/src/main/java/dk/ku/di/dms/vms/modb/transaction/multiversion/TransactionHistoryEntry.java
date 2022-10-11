package dk.ku.di.dms.vms.modb.transaction.multiversion;

public class TransactionHistoryEntry {

    public WriteType type; // if delete, record is null
    public Object[] record;

    // i just need the last version for each tid
    //public TransactionHistoryEntry previous;

    public TransactionHistoryEntry(WriteType type, Object[] record) {
        this.type = type;
        this.record = record;
    }
}
