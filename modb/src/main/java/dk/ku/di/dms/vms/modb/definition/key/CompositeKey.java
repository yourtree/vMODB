package dk.ku.di.dms.vms.modb.definition.key;

import dk.ku.di.dms.vms.modb.index.IIndexKey;

import java.util.Arrays;
import java.util.Objects;

/**
 * A sequence of values that serves both for identifying a
 * unique row (e.g., as PK) or a unique index entry.
 * The hash code is the hash of the array composed by all
 * values involved in this composition.
 */
public final class CompositeKey implements IKey, IIndexKey {

    private final Object[] values;

    public static CompositeKey of(Object[] values){
        return new CompositeKey(values);
    }

    private CompositeKey(Object[] values) {
        this.values = values;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.values);
    }

    @Override
    public boolean equals(Object key){
        return key instanceof CompositeKey compositeKey &&
            Arrays.equals(this.values, compositeKey.values);
    }

    @Override
    public String toString() {
        return "{"
                + "\"values\":" + Arrays.toString(this.values)
                + "}";
    }

}
