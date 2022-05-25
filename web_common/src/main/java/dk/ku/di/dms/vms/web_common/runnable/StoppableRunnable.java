package dk.ku.di.dms.vms.web_common.runnable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Abstract class that provides common features for server classes
 */
public abstract class StoppableRunnable implements Runnable {

    protected final Logger logger = Logger.getLogger(this.getClass().getName());

    private final AtomicBoolean state;

    public StoppableRunnable() {
        this.state = new AtomicBoolean( true );
    }

    public boolean isStopped() {
        return !state.get();
    }

    public void stop() {
        state.set(false);
    }

}
