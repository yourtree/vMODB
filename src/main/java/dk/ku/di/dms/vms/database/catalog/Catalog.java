package dk.ku.di.dms.vms.database.catalog;

import dk.ku.di.dms.vms.database.store.Table;

import java.util.HashMap;
import java.util.Map;

public class Catalog {

    public Map<String, Table<?,?>> tableMap;

    public Catalog() {
        this.tableMap = new HashMap<>();
    }
}
