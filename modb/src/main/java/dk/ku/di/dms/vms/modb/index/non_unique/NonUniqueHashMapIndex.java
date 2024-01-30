package dk.ku.di.dms.vms.modb.index.non_unique;

import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class NonUniqueHashMapIndex extends ReadWriteIndex<IKey> {

    // queue to allow concurrent inserts
    private final Map<IKey, Queue<Object[]>> store;

    public NonUniqueHashMapIndex(Schema schema, int[] columnsIndex) {
        super(schema, columnsIndex);
        this.store = new ConcurrentHashMap<>();
    }

    @Override
    public IndexTypeEnum getType() {
        return IndexTypeEnum.NON_UNIQUE;
    }

    @Override
    public int size() {
        return this.store.size();
    }

    @Override
    public boolean exists(IKey key) {
        return this.store.containsKey(key);
    }

    @Override
    public void insert(IKey key, Object[] record) {
        this.store.computeIfAbsent(key, (x)-> new ConcurrentLinkedQueue<>() );
        this.store.get(key).add(record);
    }

    @Override
    public void update(IKey key, Object[] record) {
        this.store.get(key).add(record);
    }

    @Override
    public void delete(IKey key) {
        this.store.remove(key);
    }

    @Override
    public Object[] lookupByKey(IKey key) {
        return this.store.get(key).toArray();
    }

    @Override
    public Object[] record(IRecordIterator<IKey> iterator) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Object[] record(IKey key) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public IRecordIterator<IKey> iterator(IKey[] keys) {
        throw new RuntimeException("Not implemented");
    }

    // TODO finish implementation
//    private class HashMapNonUniqueIndexIterator implements IRecordIterator<IKey> {
//
//        public HashMapNonUniqueIndexIterator(){
//
//        }
//
//        @Override
//        public IKey get() {
//            return null;
//        }
//
//        @Override
//        public void next() {
//
//        }
//
//        @Override
//        public boolean hasElement() {
//            return false;
//        }
//
//    }

    @Override
    public IRecordIterator<IKey> iterator() {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean checkCondition(IRecordIterator<IKey> iterator, FilterContext filterContext) {
        throw new RuntimeException("Not implemented");
    }

}
