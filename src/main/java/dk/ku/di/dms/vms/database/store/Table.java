package dk.ku.di.dms.vms.database.store;

import dk.ku.di.dms.vms.database.store.index.AbstractIndex;
import dk.ku.di.dms.vms.infra.AbstractEntity;

import javax.persistence.GenerationType;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Table<PK extends Serializable,TYPE extends AbstractEntity<PK>> {

    public final String name;

    public final Map<String,Column> columnMap;

    public final Map<PK,TYPE> rows;

    public final Map<? extends AbstractIndex, AbstractIndex> indexes;

    public final GenerationType generationType;

    public Table(final String name, final GenerationType generationType) {
        this.name = name;
        this.columnMap = new HashMap<>();
        this.rows = new HashMap<>();
        this.indexes = new HashMap<>();

        // the entity provides this info. should read from the annotation
        this.generationType = generationType;
    }



}
