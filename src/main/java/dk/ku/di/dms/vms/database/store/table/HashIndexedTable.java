package dk.ku.di.dms.vms.database.store.table;

import dk.ku.di.dms.vms.database.store.common.IKey;
import dk.ku.di.dms.vms.database.store.row.Row;
import dk.ku.di.dms.vms.database.store.meta.Schema;
import dk.ku.di.dms.vms.database.store.index.HashIndex;

/**
 * Map-based Table for primary key index lookup
 *
 */
public final class HashIndexedTable extends Table {

    public HashIndexedTable(final String name, final Schema schema, int... columnsIndex) {
        super(name, schema);
        this.primaryKeyIndex = new HashIndex(this, columnsIndex);
    }

    public HashIndexedTable(final String name, final Schema schema, final int initialSize, int... columnsIndex) {
        super(name, schema);
        this.primaryKeyIndex = new HashIndex(this, initialSize, columnsIndex);
    }

    @Override
    public int size() {
        return primaryKeyIndex.size();
    }

    @Override
    public boolean upsert(Row row) throws Exception {
        throw new Exception("Cannot insert into a hash without hash key");
    }

    @Override
    public boolean upsert(IKey key, Row row) {
        primaryKeyIndex.upsert(key,row);
        return true;
    }

    @Override
    public boolean delete(IKey key) {
        primaryKeyIndex.delete(key);
        return true;
    }

    @Override
    public Row retrieve(IKey key) {
        return primaryKeyIndex.retrieve(key);
    }

}
