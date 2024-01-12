package dk.ku.di.dms.vms.modb.index.non_unique;

import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class NonUniqueHashMapIndex extends AbstractIndex<IKey> implements ReadWriteIndex<IKey> {

    private final Map<IKey, Queue<Object[]>> store;

    public NonUniqueHashMapIndex(Schema schema) {
        super(schema, schema.getPrimaryKeyColumns());
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
        throw new RuntimeException("Cannot perform lookup by key in non unique hash index");
    }

}
