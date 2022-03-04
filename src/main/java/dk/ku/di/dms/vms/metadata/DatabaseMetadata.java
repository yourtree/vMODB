package dk.ku.di.dms.vms.metadata;

import dk.ku.di.dms.vms.database.catalog.Catalog;

/**
 * The metadata of metadata.
 * Contains {@link Catalog}
 */
public class DatabaseMetadata {

    public final Catalog catalog;

    public DatabaseMetadata(Catalog catalog) {
        this.catalog = catalog;
    }
}
