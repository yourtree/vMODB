package dk.ku.di.dms.vms.modb.table;

import dk.ku.di.dms.vms.modb.index.onheap.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.schema.Schema;

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
