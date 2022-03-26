package dk.ku.di.dms.vms.modb.store.index;

import dk.ku.di.dms.vms.modb.store.table.Table;
import dk.ku.di.dms.vms.modb.store.common.IKey;
import dk.ku.di.dms.vms.modb.store.row.Row;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Read @link{https://stackoverflow.com/questions/20824686/what-is-difference-between-primary-index-and-secondary-index-exactly}
 */
public class HashIndex extends AbstractIndex<IKey> {

    protected final Map<IKey, Collection<Row>> lookupMap;

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
        // TODO finish
        return true;
    }

    @Override
    public boolean delete(IKey key) {
        this.lookupMap.remove(key);
        return true;
    }

    @Override
    public Row retrieve(IKey key) {
        Iterator<Row> iterator = this.lookupMap.get(key).iterator();
        if(iterator.hasNext()){
            return iterator.next();
        }
        return null;
    }

    @Override
    public Collection<Row> retrieveCollection(IKey key) {
        return this.lookupMap.get(key);
    }

    public boolean retrieve(IKey key, Row outputRow){
        // TODO finish outputRow = this.lookupMap.getOrDefault(key, null);
        return outputRow == null;
    }

    @Override
    public int size() {
        return this.lookupMap.size();
    }

    public Collection<Row> rows(){
        return this.lookupMap.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    @Override
    public IndexDataStructureEnum getType() {
        return IndexDataStructureEnum.HASH;
    }

    // FIXME throwing exceptions is not the best idea. should think about a new index design.. perhaps through an interface
    public Set<Map.Entry<IKey,Row>> entrySet() throws UnsupportedIndexOperationException {
        throw new UnsupportedIndexOperationException();
    }

}
