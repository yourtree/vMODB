package dk.ku.di.dms.vms.modb.transaction.multiversion;

import java.util.concurrent.atomic.AtomicInteger;

public class IntegerPrimaryKeyGenerator implements IPrimaryKeyGenerator<Integer> {

    private final AtomicInteger sequencer;

    public IntegerPrimaryKeyGenerator() {
        this.sequencer = new AtomicInteger(1);
    }

    @Override
    public Integer next() {
        return this.sequencer.getAndAdd(1);
    }

}
