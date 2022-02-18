package dk.ku.di.dms.vms.database.store.index;

import dk.ku.di.dms.vms.database.store.row.IKey;
import dk.ku.di.dms.vms.database.store.row.Row;

import java.util.*;

public class HashIndex extends AbstractIndex<IKey> {

    protected final Map<IKey, Row> lookupMap;

    public HashIndex(int... columnsIndex){
        super(columnsIndex);
        this.lookupMap = new HashMap<>();
    }

    public HashIndex(final int initialSize, int... columnsIndex){
        super(columnsIndex);
        this.lookupMap = new HashMap<>(initialSize);
    }

    @Override
    public boolean upsert(IKey key, Row row) {
        lookupMap.put(key,row);
        return true;
    }

    @Override
    public boolean delete(IKey key) {
        lookupMap.replace(key,null);
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
