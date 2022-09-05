package dk.ku.di.dms.vms.modb.api.interfaces;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * A future task that does call run() to get the result
 * Simple enough for (and only for) 1 waiter and 1 producer
 * In vms case, the task thread and the vms event handler
 * Failure is not exposed to the developer since the framework is controlling the system
 * @param <V>
 */
public class VMSFutureTask<V> implements IVMsFutureCancellable<V> {

    private AtomicInteger state;
    private static final int NEW          = 0;
    private static final int COMPLETING   = 1;
    private static final int NORMAL       = 2;
    private static final int CANCELLED    = 4;

    private Thread caller;

    private Object outcome;

    public VMSFutureTask(Thread caller){
        this.caller = caller;
        this.state = new AtomicInteger(NEW);
    }

    public void set(V v) {
        if (state.compareAndSet(NEW, COMPLETING)) {
            outcome = v;
            state.compareAndSet( COMPLETING, NORMAL ); // final state
            finishCompletion();
        }
    }

    public boolean cancel() {
//        if (!(state.get() == NEW && state.compareAndSet
//                ( NEW, CANCELLED )))
//            return false;

        // it does not matter
        state.compareAndSet(state.get(), CANCELLED);
        outcome = null; // make sure return is null
        finishCompletion();

        return true;
    }

    private void finishCompletion() {
        LockSupport.unpark(caller);
    }

    private int awaitDone(long timeout){
        long startTime = System.nanoTime();
        if (startTime == 0L)
            startTime = 1L;
        for (;;) {
            int s = state.get();
            if (s > COMPLETING) {
                return s;
            } else if (s == COMPLETING) {
                do{
                    try { Thread.sleep(100); } catch (InterruptedException ignored) {}
                    s = state.get();
                } while (s == COMPLETING);
            } else {
                long now = System.currentTimeMillis();
                long elapsed = now - startTime;
                if (elapsed >= timeout) {
                    return s;
                }
            }

        }

    }

    private int awaitDone(){

        for (;;) {
            int s = state.get();
            if (s > COMPLETING) {
                return s;
            } else if (s == COMPLETING) {
                do{
                    try { Thread.sleep(100); } catch (InterruptedException ignored) {}
                    s = state.get();
                } while (s == COMPLETING);
            }
            else
                LockSupport.park(this);

        }

    }

    @SuppressWarnings("unchecked")
    private V report(int s){
        Object x = outcome;
        if (s == NORMAL)
            return (V)x;
        // case cancelled enters here
        return null;
    }

    @Override
    public V get() {
        int s = state.get();
        if (s <= COMPLETING)
            s = awaitDone();
        return report(s);
    }

    @Override
    public V get(long timeout){
        int s = awaitDone(timeout);
        if(s < COMPLETING)
            cancel();
        return report(s);
    }

    @Override
    public boolean isDone() {
        return state.get() == NEW;
    }

    static {
        // https://bugs.openjdk.java.net/browse/JDK-8074773
        Class<?> ensureLoaded = LockSupport.class;
    }

    @Override
    public boolean isCancelled() {
        return state.get() == CANCELLED;
    }
}
