package dk.ku.di.dms.vms.database.store;

import dk.ku.di.dms.vms.infra.AbstractEntity;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Table<PK extends Serializable,TYPE extends AbstractEntity<PK>> {

    public final String name;

    public final Map<String,Column> columnMap;

    public final Map<PK,TYPE> rows;

    public final Map<? extends AbstractIndex, Map> indexes;

    public Table(final String name) {
        this.name = name;
        this.columnMap = new HashMap<>();
        this.rows = new HashMap<>();
        this.indexes = new HashMap<>();
    }



}
