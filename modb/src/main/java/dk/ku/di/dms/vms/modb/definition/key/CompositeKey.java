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

    private final Object[] value;

    public static CompositeKey of(Object[] values){
        return new CompositeKey(values);
    }

    private CompositeKey(Object[] values) {
        this.value = values;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.value);
    }

    @Override
    public boolean equals(Object key){
        return key instanceof CompositeKey compositeKey &&
            Arrays.equals(this.value, compositeKey.value);
    }

    @Override
    public String toString() {
        return "{"
                + "\"value\":" + Arrays.toString(this.value)
                + "}";
    }

    public Object[] getValue() {
        return value;
    }

}
