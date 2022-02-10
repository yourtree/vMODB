package dk.ku.di.dms.vms.database.store.row;

import java.util.Arrays;

public class CompositeKey extends Row implements IKey {

    public CompositeKey(Object... values) {
        super(values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

}
