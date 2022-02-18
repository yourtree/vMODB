package dk.ku.di.dms.vms.database.store.index;

import dk.ku.di.dms.vms.database.store.meta.DataType;
import dk.ku.di.dms.vms.database.store.row.Row;

import java.util.*;

/**
 * Basic implementation of a range index
 * Given a column and a data type
 */
public class RangeIndex<T> extends AbstractIndex<T> {

    // TODO a range index can also be created using a condition

    final SortedMap<T,Collection<Row>> map;

    final int columnIndex;

    public RangeIndex(final DataType dataType,final int columnIndex){
        super(columnIndex);
        this.map = this.getMap(dataType);
        this.columnIndex = columnIndex;
    }

    private SortedMap<T,Collection<Row>> getMap(final DataType dataType){

        switch(dataType){
            case INT:
                return (SortedMap<T, Collection<Row>>) new TreeMap<Integer,Collection<Row>>();
            case STRING:
                return (SortedMap<T, Collection<Row>>) new TreeMap<String,Collection<Row>>();
            case CHAR:
                return (SortedMap<T, Collection<Row>>) new TreeMap<Character,Collection<Row>>();
            case LONG:
                return (SortedMap<T, Collection<Row>>) new TreeMap<Long,Collection<Row>>();
            case DOUBLE:
                return (SortedMap<T, Collection<Row>>) new TreeMap<Double,Collection<Row>>();
            default:
                throw new IllegalStateException("Unexpected value: " + dataType);
        }
    }


    @Override
    public boolean upsert(T key, Row row) {
        // TODO finish
        return true;
    }

    @Override
    public boolean delete(T key) {
        map.replace(key,null);
        return true;
    }

    @Override
    public Row retrieve(T key) {
        // VERY BAD!!!! the semantics of the interface does not help.
        return map.get(key).stream().findFirst().get();
    }

    @Override
    public boolean retrieve(T key, Row outputRow) {
        return false;
    }

    @Override
    public int size() {
        // TODO actually should not be map.size(), but a counter that I should maintain
        return map.size();
    }

    @Override
    public Collection<Row> rows() {
        // for a range index, this is an expensive operation
        // all collections require being aggregated
        // this index should be avoided in case no index can be applied
        return null; // TODO finish map.values().stream().collect(Collectors.toList());
    }

    @Override
    public IndexDataStructureEnum getType() {
        return IndexDataStructureEnum.TREE;
    }

    @Override
    public Set<Map.Entry<T, Row>> entrySet() {
        return null;
    }


}
