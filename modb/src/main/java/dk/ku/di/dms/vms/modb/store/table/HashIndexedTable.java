package dk.ku.di.dms.vms.modb.store.table;

import dk.ku.di.dms.vms.modb.store.index.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.store.meta.Schema;

/**
 * Map-based Table for primary key index lookup
 *
 */
public final class HashIndexedTable extends Table {

    public HashIndexedTable(final String name, final Schema schema) {
        super(name, schema);
        this.primaryKeyIndex = new UniqueHashIndex(this, schema.getPrimaryKeyColumns());
    }

    public HashIndexedTable(final String name, final Schema schema, final int initialSize) {
        super(name, schema);
        this.primaryKeyIndex = new UniqueHashIndex(this, initialSize, schema.getPrimaryKeyColumns());
    }

}
