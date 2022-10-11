package dk.ku.di.dms.vms.modb.definition;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.modb.index.ReadWriteIndex;

import java.util.HashMap;
import java.util.Map;

/**
 * A catalog usually stores tables, views, columns, triggers and procedures in a DBMS
 * See <a href="https://en.wikipedia.org/wiki/Oracle_metadata">Oracle Metadata</a>
 * In our case, the table already stores the columns
 * We don't have triggers nor stored procedures =)
 * For now, we don't have views, but we can implement the application-defined
 * queries as views and store them here
 */
public final class Catalog {

    private final Map<String, Table> tableMap;

    private final Map<IIndexKey, ReadWriteIndex<IKey>> indexMap;

    public Catalog() {
        this.tableMap = new HashMap<>();
        this.indexMap = new HashMap<>();
    }

    public Table getTable(String tableName) {
        return this.tableMap.getOrDefault(tableName,null);
    }

    public void insertTable(Table table){
        this.tableMap.put(table.getName(),table);
    }

    public void insertTables(Table... tables){
        for(Table table : tables) {
            this.tableMap.put(table.getName(), table);
        }
    }

    public void insertIndex(IIndexKey indexKey, ReadWriteIndex<IKey> index){
        this.indexMap.put(indexKey, index);
    }

    public ReadWriteIndex<IKey> getIndexByKey(IIndexKey indexKey){
        return this.indexMap.get(indexKey);
    }

    public Map<IIndexKey, ReadWriteIndex<IKey>> indexMap(){
        return this.indexMap;
    }

}
