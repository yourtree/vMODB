package dk.ku.di.dms.vms.database.store.meta;

import dk.ku.di.dms.vms.infra.AbstractEntity;

public class ForeignKeyReference {

    private final Class<? extends AbstractEntity<?>> entityClazz;

    // the position of the columns that reference the other table
    private final String columnName;

    // private CardinalityTypeEnum cardinality; TODO check whether this is necessary

    public ForeignKeyReference(final Class<? extends AbstractEntity<?>> entityClazz, final String columnName) {
        this.entityClazz = entityClazz;
        this.columnName = columnName;
    }

    public Class<? extends AbstractEntity<?>> getEntity() {
        return entityClazz;
    }

    public String getColumnName() {
        return columnName;
    }

}
