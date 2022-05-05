package dk.ku.di.dms.vms.web_common.runnable;

import dk.ku.di.dms.vms.modb.common.interfaces.IVmsFuture;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * A future task that does call run() to get the result
 * Simple enough for (and only for) 1 waiter and 1 producer
 * In vms case, the task thread and the vms event handler
 * Failure is not exposed to the developer since the framework is controlling the system
 * @param <V>
 */
public class VMSFutureTask<V> implements IVmsFuture<V> {

    private AtomicInteger state;
    private static final int NEW          = 0;
    private static final int COMPLETING   = 1;
    private static final int NORMAL       = 2;

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

    private void finishCompletion() {
        LockSupport.unpark(caller);
    }

    private int awaitDone(){

        for (;;) {
            int s = state.get();
            if (s > COMPLETING) {
                return s;
            } else if (s == COMPLETING) {
                do{
                    try { Thread.sleep(100); } catch (InterruptedException e) {}
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
        return  null;
    }

    @Override
    public V get() {
        int s = state.get();
        if (s <= COMPLETING)
            s = awaitDone();
        return report(s);
    }

    @Override
    public boolean isDone() {
        return false;
    }

    static{
        // https://bugs.openjdk.java.net/browse/JDK-8074773
        Class<?> ensureLoaded = LockSupport.class;
    }

}
