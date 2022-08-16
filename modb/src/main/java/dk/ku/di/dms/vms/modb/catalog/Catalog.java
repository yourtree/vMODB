package dk.ku.di.dms.vms.modb.catalog;

import dk.ku.di.dms.vms.modb.table.Table;

import java.util.HashMap;
import java.util.Map;

/**
 * A catalog usually stores tables, views, columns, triggers and procedures in a DBMS
 * See https://en.wikipedia.org/wiki/Oracle_metadata
 * In our case, the table already stores the columns
 * We don't have triggers nor stored procedures =)
 * For now, we don't have views, but we can implement the application-defined
 * queries as views and store hem here
 */
public final class Catalog {

    private Map<Integer, Table> tableMap_;
    private Map<String, Table> tableMap;

    public Catalog() {
        this.tableMap = new HashMap<>();
    }

    public Table getTable(int tableId) {
        return tableMap.get(tableId);
    }

    public Table getTable(String tableName) {
        return tableMap.getOrDefault(tableName,null);
    }

    public boolean insertTable(Table table){
        tableMap.put(table.getName(),table);
        return true;
    }

    public boolean insertTables(Table... tables){
        for(Table table : tables) {
            tableMap.put(table.getName(), table);
        }
        return true;
    }

}
