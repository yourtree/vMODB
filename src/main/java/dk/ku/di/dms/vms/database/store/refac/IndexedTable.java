package dk.ku.di.dms.vms.database.store.refac;

import java.util.Iterator;
import java.util.Map;

/**
 * Map-based Table for primary key index lookup
 */
public class IndexedTable extends Table {

    private final Map<IKey, Row> lookupMap;

    // index of the keys in the column array, in order
    private final int[] posKeyColumns;

    public IndexedTable(String name, Schema schema, Map<IKey, Row> lookupMap, int[] posKeyColumns) {
        super(columnNameToIndexMap, name, schema);
        this.lookupMap = lookupMap;
        this.posKeyColumns = posKeyColumns;
    }

    @Override
    public int size() {
        return lookupMap.size();
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
    public Iterator<Row> iterator() {
        return lookupMap.values().iterator();
    }
}
