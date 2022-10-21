package dk.ku.di.dms.vms.modb.transaction.multiversion;

import java.util.concurrent.atomic.AtomicLong;

public class LongPrimaryKeyGenerator implements IPrimaryKeyGenerator<Long> {

    private AtomicLong sequencer;

    public LongPrimaryKeyGenerator() {
        this.sequencer = new AtomicLong(0);
    }

    @Override
    public Long next() {
        return sequencer.incrementAndGet();
    }

}
