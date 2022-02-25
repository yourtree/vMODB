package dk.ku.di.dms.vms.database.store.common;

import dk.ku.di.dms.vms.database.store.index.IIndexKey;
import dk.ku.di.dms.vms.database.store.row.Row;

import java.util.Arrays;

public class CompositeKey extends Row implements IKey, IIndexKey {

    public CompositeKey(Object... values) {
        super(values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

}
