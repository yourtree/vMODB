package dk.ku.di.dms.vms.modb.transaction.multiversion.index;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyIndex;

public interface IMultiVersionIndex extends ReadOnlyIndex<IKey> {

    void undoTransactionWrites();

    void installWrites();

}
