package dk.ku.di.dms.vms.modb.transaction.multiversion.index;

import dk.ku.di.dms.vms.modb.definition.key.IKey;

public interface IMultiVersionIndex {

    void undoTransactionWrites();

    void installWrites();

    boolean insert(IKey key, Object[] record);

    boolean update(IKey key, Object[] record);

    boolean remove(IKey key);

    Object[] lookupByKey(IKey key);

}
