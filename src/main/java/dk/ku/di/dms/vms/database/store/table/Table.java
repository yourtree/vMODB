package dk.ku.di.dms.vms.database.store.table;

import dk.ku.di.dms.vms.database.store.row.IKey;
import dk.ku.di.dms.vms.database.store.row.Row;
import dk.ku.di.dms.vms.database.store.meta.Schema;
import dk.ku.di.dms.vms.database.store.index.IIndex;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public abstract class Table {

    protected final String name;

    protected final Schema schema;

    protected IIndex primaryIndex;

    protected Map<IKey, IIndex> secondaryIndexes;

    public abstract int size();

    /** no primary index */
    public abstract boolean upsert(Row row) throws Exception;

    public abstract boolean upsert(IKey key, Row row);

    public abstract boolean delete(IKey key);

    public abstract Row retrieve(IKey key);

    /** Iterate over primary key if exists */
    public Iterator<Row> iterator(){
        return hasPrimaryKey() ? this.primaryIndex.iterator() : rows().iterator();
    }

    /** Iterate over a chosen index, if exists */
    public Iterator<Row> iterator(IKey indexKey){
        IIndex opt = this.secondaryIndexes.getOrDefault(indexKey,null);
        return opt != null ? opt.iterator() : rows().iterator();
    }

    public abstract Collection<Row> rows();

    public Table(String name, Schema schema) {
        this.name = name;
        this.schema = schema;
    }

    public Schema getSchema(){
        return schema;
    }

    public boolean hasPrimaryKey(){
        return primaryIndex != null;
    }

    //********//

//    @Override
//    public boolean upsert(Row row) {
//        return false;
//    }
//
//    @Override
//    public boolean upsert(IKey key, Row row) {
//        lookupMap.put(key,row);
//        return true;
//    }
//
//    @Override
//    public boolean delete(IKey key) {
//        lookupMap.replace(key,null);
//        return true;
//    }
//
//    @Override
//    public Row retrieve(IKey key) {
//        return lookupMap.get(key);
//    }
//
//    @Override
//    public Iterator<Row> iterator() {
//        return lookupMap.values().iterator();
//    }
//
//    @Override
//    public Collection<Row> rows() {
//        return lookupMap.values();
//    }

        public String getName(){
            return this.name;
        }
}
