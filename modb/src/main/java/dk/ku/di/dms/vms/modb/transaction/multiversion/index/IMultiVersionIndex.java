package dk.ku.di.dms.vms.modb.transaction.multiversion.index;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyIndex;

public interface IMultiVersionIndex extends ReadOnlyIndex<IKey> {

    void undoTransactionWrites();

    void installWrites();

    boolean insert(IKey key, Object[] record);

    boolean update(IKey key, Object[] record);

    boolean delete(IKey key);

}
