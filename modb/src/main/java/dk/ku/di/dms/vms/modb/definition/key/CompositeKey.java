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
    // private final int hashKey;

    public static CompositeKey of(Object[] values){
        return new CompositeKey(values);
    }

    public static CompositeKey of(int[] values){
        return new CompositeKey(Arrays.stream(values).mapToObj(Objects::toString).toArray());
    }

    private CompositeKey(Object[] values) {
        this.value = values;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public boolean equals(Object key){
        return key instanceof CompositeKey compositeKey &&
                // this.value.equals(compositeKey.value);
        Arrays.equals(value, compositeKey.value);
                /*
               equals((byte[]) STRING_VALUE_HANDLE.get(this.value),
                       (byte[]) STRING_VALUE_HANDLE.get(compositeKey.value));

                 */
    }

    public static boolean equals(byte[] value, byte[] other) {
        if (value.length == other.length) {
            for (int i = 0; i < value.length; i++) {
                if (value[i] != other[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "{"
                + "\"value\":" + Arrays.toString(value)
                + "}";
    }

}
