package dk.ku.di.dms.vms.web_common.runnable;

/**
 * Abstract class that provides common features for server classes
 */
public abstract class StoppableRunnable implements Runnable {

    private volatile boolean running;

    public StoppableRunnable() {
        this.running = true;
    }

    public boolean isStopped() {
        return !running;
    }

    public void stop() {
        running = false;
    }

}
