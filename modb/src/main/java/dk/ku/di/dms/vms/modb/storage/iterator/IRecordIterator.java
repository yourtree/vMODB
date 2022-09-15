package dk.ku.di.dms.vms.modb.storage.iterator;

import dk.ku.di.dms.vms.modb.definition.key.IKey;

import java.util.Iterator;

public interface IRecordIterator extends Iterator<Long> {

    // depending on the type of index, the primary key may be positioned in different address space
    IKey primaryKey();

    long current();

}
