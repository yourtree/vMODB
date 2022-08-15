package dk.ku.di.dms.vms.modb.table;

import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.schema.Schema;

/**
 * Hash-based table for primary-key index lookup
 *
 */
public final class HashIndexedTable extends Table {

    public HashIndexedTable(final String name, final Schema schema) {
        super(name, schema);
        this.primaryKeyIndex = new UniqueHashIndex(null, this, schema.getPrimaryKeyColumns());
    }

}
