package dk.ku.di.dms.vms.modb.transaction.multiversion;

public class TransactionWrite {

    public WriteType type; // if delete, record is null
    public Object[] record; // the whole record

    // i just need the last version for each tid
    //public TransactionHistoryEntry previous;

    public TransactionWrite(WriteType type, Object[] record) {
        this.type = type;
        this.record = record;
    }
}
