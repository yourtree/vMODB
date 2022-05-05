package dk.ku.di.dms.vms.web_common.runnable;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class StoppableRunnable implements Runnable {

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
