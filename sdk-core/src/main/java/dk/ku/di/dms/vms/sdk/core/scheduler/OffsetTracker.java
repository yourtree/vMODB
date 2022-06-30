package dk.ku.di.dms.vms.sdk.core.scheduler;

class OffsetTracker {

    enum OffsetStatus {
        INIT,
        READY,
        EXECUTING,
        FINISHED
    }

    private final int tid;

    // how many tasks from this tid remain to be scheduled?
    private int remainingReadyTasks;

    // how many tasks from this tid remain to execute?
    private int remainingFinishedTasks;

    private OffsetStatus status;

    public OffsetTracker(int tid, int remainingReadyTasks) {
        this.tid = tid;
        this.remainingReadyTasks = remainingReadyTasks;
        this.remainingFinishedTasks = remainingReadyTasks;
        this.status = OffsetStatus.INIT;
    }

    private void moveToReadyState(){
        this.status = OffsetStatus.READY;
    }

    private void moveToDoneState(){
        this.status = OffsetStatus.FINISHED;
    }

    public void moveToExecutingState(){
        this.status = OffsetStatus.EXECUTING;
    }

    // constraints in objects would be great! e.g., remainingTasks >= 0 always
    public void signalReady(){
        if(this.remainingReadyTasks == 0) throw new RuntimeException("Cannot have below zero remaining tasks.");
        this.remainingReadyTasks--;
        if(remainingReadyTasks == 0) moveToReadyState();
    }

    public void signalFinished(){
        if(this.remainingFinishedTasks-1 < this.remainingReadyTasks) throw new RuntimeException("Cannot have finished tasks lower than ready tasks.");
        if(this.remainingFinishedTasks == 0) throw new RuntimeException("Cannot have below zero remaining tasks.");
        this.remainingFinishedTasks--;
        if(remainingFinishedTasks == 0) moveToDoneState();
    }

    public int tid() {
        return this.tid;
    }

    public OffsetStatus status(){
        return this.status;
    }
}
