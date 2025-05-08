package dk.ku.di.dms.vms.modb.definition.key.composite;

import dk.ku.di.dms.vms.modb.definition.key.IKey;

public final class PairCompositeKey extends BaseComposite implements IKey {

    private final Object value0;
    private final Object value1;

    public static PairCompositeKey of(Object value0, Object value1){
        return new PairCompositeKey(value0, value1);
    }

    private PairCompositeKey(Object value0, Object value1) {
        super(hashCode(value0, value1));
        this.value0 = value0;
        this.value1 = value1;
    }

    private static int hashCode(Object value0, Object value1) {
        int value = STR."\{value0}|\{value1}".hashCode();
        return value < 0 ? value & 0x7fffffff : value;
    }

    @Override
    public boolean equals(Object key){
        return key instanceof PairCompositeKey pairCompositeKey &&
                this.value0.equals(pairCompositeKey.value0) &&
                this.value1.equals(pairCompositeKey.value1);
    }

    @Override
    public String toString() {
        return "{"
                + "\"value0\":" + this.value0
                + ",\"value1\":" + this.value1
                + "}";
    }

}
