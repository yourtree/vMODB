package dk.ku.di.dms.vms.database.store.index;

import dk.ku.di.dms.vms.database.store.row.IKey;
import dk.ku.di.dms.vms.database.store.row.Row;

import java.util.*;

public class HashIndex implements IIndex {

    protected final Map<IKey, Row> lookupMap;

    // I need this to figure out whether this index
    private final int[] columnsIndex;

    public HashIndex(int... columnsIndex) {
        this.lookupMap = new HashMap<>();
        this.columnsIndex = columnsIndex;
    }

    public HashIndex(final int initialSize, int... columnsIndex){
        this.lookupMap = new HashMap<>(initialSize);
        this.columnsIndex = columnsIndex;
    }

    // use bitwise comparison to find whether a given index exists for such columns
    // https://stackoverflow.com/questions/8504288/java-bitwise-comparison-of-a-byte/8504393
    public int hashCode(){
        if (columnsIndex.length == 1) return columnsIndex[0];
        return Arrays.hashCode(columnsIndex);
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

    // clustered index does not make sense in mmdbs?

    // default is hash
    // public IndexEnum indexType;

    @Override
    public int size() {
        return lookupMap.size();
    }

    public Collection<Row> rows(){
        return lookupMap.values();
    }

    public Set<Map.Entry<IKey,Row>> entrySet(){
        return lookupMap.entrySet();
    }

}
