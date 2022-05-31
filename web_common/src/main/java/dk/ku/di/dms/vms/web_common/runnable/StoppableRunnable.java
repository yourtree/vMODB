package dk.ku.di.dms.vms.web_common.runnable;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import static dk.ku.di.dms.vms.web_common.runnable.Constants.NO_RESULT;

/**
 * Abstract class that provides common features for server classes
 */
public abstract class StoppableRunnable implements Runnable {

    protected final Logger logger = Logger.getLogger(this.getClass().getName());

    public final CustomUncaughtExceptionHandler exceptionHandler;

    // queue serves as a channel to respond the calling thread
    protected final BlockingQueue<Byte> signal;

    private final AtomicBoolean state;

    public StoppableRunnable() {
        this.state = new AtomicBoolean( true );
        this.signal = new ArrayBlockingQueue<>(1);
        this.exceptionHandler = new CustomUncaughtExceptionHandler( signal );
    }

    /**
     * Blocks the caller until this thread finishes its work.
     * @return A byte representing the result
     */
    public Byte getResult() throws InterruptedException {
        return signal.take();
    }

    public boolean isStopped() {
        return !state.get();
    }

    public void stop() {
        state.set(false);
    }

    private class CustomUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

        private final BlockingQueue<Byte> signal;

        public CustomUncaughtExceptionHandler(BlockingQueue<Byte> signal){
            this.signal = signal;
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            this.signal.add( NO_RESULT );
            logger.info( e.getLocalizedMessage() );
        }
    }

}
