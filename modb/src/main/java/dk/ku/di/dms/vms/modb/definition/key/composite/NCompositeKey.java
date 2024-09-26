package dk.ku.di.dms.vms.modb.definition.key.composite;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IIndexKey;

import java.util.Arrays;
import java.util.Objects;

/**
 * A sequence of values that serves both for identifying a
 * unique row (e.g., as PK) or a unique index entry.
 * The hash code is the hash of the array composed by all
 * values involved in this composition.
 */
public final class NCompositeKey extends BaseComposite implements IKey, IIndexKey {

    private final Object[] values;

    public static NCompositeKey of(Object[] values){
        return new NCompositeKey(values);
    }

    private NCompositeKey(Object[] values) {
        super(Objects.hash(values));
        this.values = values;
    }

    @Override
    public boolean equals(Object key){
        return key instanceof NCompositeKey compositeKey &&
            Arrays.equals(this.values, compositeKey.values);
    }

    @Override
    public String toString() {
        return "{"
                + "\"values\":" + Arrays.toString(this.values)
                + "}";
    }

}
