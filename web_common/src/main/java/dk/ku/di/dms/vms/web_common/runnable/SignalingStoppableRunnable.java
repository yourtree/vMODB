package dk.ku.di.dms.vms.web_common.runnable;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import static dk.ku.di.dms.vms.web_common.runnable.Constants.NO_RESULT;

/**
 * Abstract class that provides common features for server classes
 */
public abstract class SignalingStoppableRunnable implements Runnable {

    protected final Logger logger = Logger.getLogger(this.getClass().getName());

    public final CustomUncaughtExceptionHandler exceptionHandler;

    // queue serves as a channel to respond the calling thread
    protected final BlockingQueue<Byte> signal;

    private volatile boolean running;

    public SignalingStoppableRunnable() {
        this.running = true;
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
        return !running;
    }

    public void stop() {
        running = false;
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
