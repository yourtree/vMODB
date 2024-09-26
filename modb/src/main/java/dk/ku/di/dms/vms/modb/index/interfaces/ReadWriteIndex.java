package dk.ku.di.dms.vms.modb.index.interfaces;

import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;

public abstract class ReadWriteIndex<K> extends AbstractIndex<K> {

    public ReadWriteIndex(Schema schema, int[] columnsIndex) {
        super(schema, columnsIndex);
    }

    public abstract void insert(IKey key, Object[] record);

    public abstract void update(IKey key, Object[] record);

    public abstract void delete(IKey key);

    public abstract Object[] lookupByKey(IKey key);

    public boolean exists(IKey key, Object[] record) { return false; }

    public void upsert(IKey key, Object[] record) { this.insert(key, record); }

    public void reset() {
        throw new RuntimeException("Not supported.");
    }

    // flush updates
    public void flush(){
        throw new RuntimeException("Not supported.");
    }

}
