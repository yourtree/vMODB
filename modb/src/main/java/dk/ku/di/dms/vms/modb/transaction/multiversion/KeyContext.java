package dk.ku.di.dms.vms.modb.transaction.multiversion;

public class KeyContext {

    public TransactionWrite first;

    public TransactionWrite last;

    public WriteType lastWriteType;

    public Object[] cachedEntity;

    public KeyContext(){ }

}