package dk.ku.di.dms.vms.database.store.index;

import dk.ku.di.dms.vms.database.store.row.IKey;
import dk.ku.di.dms.vms.database.store.row.Row;

import java.util.Collection;
import java.util.Iterator;

public interface IIndex {

    int hashCode();

    boolean upsert(IKey key, Row row);

    boolean delete(IKey key);

    Row retrieve(IKey key);

    Iterator<Row> iterator();

    int size();

    Collection<Row> rows();

}
