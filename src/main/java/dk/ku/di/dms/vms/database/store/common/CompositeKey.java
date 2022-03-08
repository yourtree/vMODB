package dk.ku.di.dms.vms.database.store.common;

import dk.ku.di.dms.vms.database.store.index.IIndexKey;
import dk.ku.di.dms.vms.database.store.row.Row;

import java.util.Arrays;

public class CompositeKey extends Row implements IKey, IIndexKey {

    private final int hashKey;

    public CompositeKey(Object... values) {
        super(values);
        this.hashKey = Arrays.hashCode(values);
    }

    @Override
    public int hashCode() {
        return hashKey;
    }

    @Override
    public boolean equals(Object key){
        // if(this.hashKey == ((CompositeKey)key).hashKey) return true;
        return this.hashCode() == key.hashCode();
//        return false;
    }

}
