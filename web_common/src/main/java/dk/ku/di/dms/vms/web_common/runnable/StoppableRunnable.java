package dk.ku.di.dms.vms.web_common.runnable;

import java.util.logging.Logger;

/**
 * Abstract class that provides common features for server classes
 */
public abstract class StoppableRunnable implements Runnable {

    protected final Logger logger;

    private volatile boolean running;

    public StoppableRunnable() {
        // starts running as default
        this.running = true;
        this.logger = Logger.getLogger(getClass().getSimpleName());
    }

    public boolean isRunning() {
        return this.running;
    }

    public void stop() {
        this.running = false;
    }

}
