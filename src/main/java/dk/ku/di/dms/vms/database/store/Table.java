package dk.ku.di.dms.vms.database.store;

import dk.ku.di.dms.vms.infra.AbstractEntity;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class Table<PK extends Serializable,TYPE extends AbstractEntity> {

    public String name;

    public Map<String,Column> columnMap;

    public Map<PK,TYPE> rows;

    public Map<? extends AbstractIndex, Map> indexes;

    public Table() {
        this.columnMap = new HashMap<>();
//        Class<TYPE> clazz;
//        clazz.
    }

}
