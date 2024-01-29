package dk.ku.di.dms.vms.modb.index.unique;

import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class UniqueHashMapIndex extends ReadWriteIndex<IKey> {

    private final Map<IKey,Object[]> store;

    public UniqueHashMapIndex(Schema schema) {
        super(schema, schema.getPrimaryKeyColumns());
        this.store = new ConcurrentHashMap<>();
    }

    @Override
    public IndexTypeEnum getType() {
        return IndexTypeEnum.UNIQUE;
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
        this.store.putIfAbsent(key, record);
    }

    @Override
    public void update(IKey key, Object[] record) {
        this.store.put(key, record);
    }

    @Override
    public void delete(IKey key) {
        this.store.remove(key);
    }

    @Override
    public Object[] lookupByKey(IKey key) {
        return this.store.get(key);
    }

    @Override
    public Object[] record(IRecordIterator<IKey> iterator) {
        return new Object[0];
    }

    @Override
    public IRecordIterator<IKey> iterator() {
        return null;
    }

    @Override
    public boolean checkCondition(IRecordIterator<IKey> iterator, FilterContext filterContext) {
        return false;
    }
}
