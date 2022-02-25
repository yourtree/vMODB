package dk.ku.di.dms.vms.database.store.index;

import dk.ku.di.dms.vms.database.store.common.IKey;
import dk.ku.di.dms.vms.database.store.row.Row;
import dk.ku.di.dms.vms.database.store.table.Table;

import java.util.*;

/**
 * Read @link{https://stackoverflow.com/questions/20824686/what-is-difference-between-primary-index-and-secondary-index-exactly}
 */
public class HashIndex extends AbstractIndex<IKey> {

    protected final Map<IKey, Row> lookupMap;

    public HashIndex(final Table table, int... columnsIndex){
        super(table, columnsIndex);
        this.lookupMap = new HashMap<>();
    }

    public HashIndex(final Table table, final int initialSize, int... columnsIndex){
        super(table, columnsIndex);
        this.lookupMap = new HashMap<>(initialSize);
    }

    @Override
    public boolean upsert(IKey key, Row row) {
        lookupMap.put(key,row);
        return true;
    }

    @Override
    public boolean delete(IKey key) {
        lookupMap.remove(key);
        return true;
    }

    @Override
    public Row retrieve(IKey key) {
        return lookupMap.get(key);
    }

    public boolean retrieve(IKey key, Row outputRow){
        outputRow = lookupMap.getOrDefault(key, null);
        return outputRow == null;
    }

    @Override
    public int size() {
        return lookupMap.size();
    }

    public Collection<Row> rows(){
        return lookupMap.values();
    }

    @Override
    public IndexDataStructureEnum getType() {
        return IndexDataStructureEnum.HASH;
    }

    public Set<Map.Entry<IKey,Row>> entrySet(){
        return lookupMap.entrySet();
    }

}
