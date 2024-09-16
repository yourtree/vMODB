package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

@SuppressWarnings("NullableProblems")
public class CustomBlockingQueue extends ConcurrentLinkedQueue<TransactionEvent.PayloadRaw> implements BlockingQueue<TransactionEvent.PayloadRaw> {

    private final AtomicInteger batchSize = new AtomicInteger(0);

    private volatile AtomicBoolean unparkPermission = new AtomicBoolean(false);

    private transient Thread waiter;

    @SuppressWarnings("NullableProblems")
    @Override
    public boolean add(TransactionEvent.PayloadRaw payloadRaw) {
        super.offer(payloadRaw);
        batchSize.getAndAdd( payloadRaw.totalSize() );
        // signal there is at least one
        if(waiter != null && waiter.getState() == Thread.State.WAITING) {
            if(unparkPermission.compareAndExchange(false, true)) {
                LockSupport.unpark(waiter);
                unparkPermission.set(false);
            }
        }
        return true;
    }

    public void registerAsWaiter(){
        this.waiter = Thread.currentThread();
    }

    @Override
    public int drainTo(Collection<? super TransactionEvent.PayloadRaw> c, int maxSize) {
        // sleep
        if(batchSize.get() == 0) {
            LockSupport.park();
            // woke
        }
        // has the batch size reached?
        if(batchSize.get() < maxSize){
            // long start = System.currentTimeMillis();
            LockSupport.parkNanos( 300 );
        }
        TransactionEvent.PayloadRaw event_;
        int sizeRemoved = 0;
        while((event_ = this.peek()) != null && (sizeRemoved + event_.totalSize()) < maxSize){
            c.add(this.poll());
            sizeRemoved = sizeRemoved + event_.totalSize();
        }
        batchSize.addAndGet( -sizeRemoved );
        return sizeRemoved;
    }

    @Override
    public void put(TransactionEvent.PayloadRaw payloadRaw) throws InterruptedException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean offer(TransactionEvent.PayloadRaw payloadRaw, long timeout, TimeUnit unit) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public TransactionEvent.PayloadRaw take() {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public TransactionEvent.PayloadRaw poll(long timeout, TimeUnit unit) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public int remainingCapacity() {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public int drainTo(Collection<? super TransactionEvent.PayloadRaw> c) {
        throw new RuntimeException("Not implemented");
    }

}
