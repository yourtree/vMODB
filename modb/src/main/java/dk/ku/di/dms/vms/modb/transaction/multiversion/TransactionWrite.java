package dk.ku.di.dms.vms.modb.transaction.multiversion;

public class TransactionWrite {

    public WriteType type; // if delete operation, record is null
    public Object[] record; // the whole record

    public TransactionWrite(WriteType type, Object[] record) {
        this.type = type;
        this.record = record;
    }
}
