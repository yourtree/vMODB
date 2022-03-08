package dk.ku.di.dms.vms.database.store.common;

import dk.ku.di.dms.vms.database.store.index.IIndexKey;

public class SimpleKey implements IKey, IIndexKey {

    private final Object value;

    public SimpleKey(Object value) {
        this.value = value;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object key){
        return this.hashCode() == key.hashCode();
    }

}
