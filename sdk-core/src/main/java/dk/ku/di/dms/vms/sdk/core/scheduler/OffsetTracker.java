package dk.ku.di.dms.vms.sdk.core.scheduler;

class OffsetTracker {

    enum OffsetStatus {
        NEW,
        // READY, does not make much sense. different tasks may be scheduled beforehand. don't need to wait for all inputs of a certain tasks to start this (transaction) offset
        // EXECUTING, // also does not make sense, since there may be many
        FINISHED_SUCCESSFULLY,
        FINISHED_WITH_ERROR
    }

    private final long tid;

    // how many tasks from this tid remain to be scheduled?
    //private int remainingReadyTasks;

    // how many tasks from this tid remain to execute?
    private int remainingFinishedTasks;

    private OffsetStatus status;

    public OffsetTracker(long tid, int numberOfTasks) {
        this.tid = tid;
        //this.remainingReadyTasks = remainingReadyTasks;
        this.remainingFinishedTasks = numberOfTasks;
        this.status = OffsetStatus.NEW;
    }

//    private void moveToReadyState(){
//        this.status = OffsetStatus.READY;
//    }

    private void moveToDoneState(){
        this.status = OffsetStatus.FINISHED_SUCCESSFULLY;
    }

//    public void moveToExecutingState(){
//        this.status = OffsetStatus.EXECUTING;
//    }

    private void moveToErrorState() { this.status = OffsetStatus.FINISHED_WITH_ERROR; }

    // constraints in objects would be great! e.g., remainingTasks >= 0 always
//    public void signalReady(){
//        // if(this.remainingReadyTasks == 0) throw new RuntimeException("Cannot have below zero remaining tasks.");
//        assert this.remainingReadyTasks > 0;
//        this.remainingReadyTasks--;
//        // if(remainingReadyTasks == 0) moveToReadyState();
//    }

    public void signalTaskFinished(){
        // if( (this.remainingFinishedTasks - 1) < this.remainingReadyTasks) throw new RuntimeException("Cannot have finished tasks lower than ready tasks.");
//        assert this.remainingFinishedTasks > this.remainingReadyTasks;
        assert this.remainingFinishedTasks > 0;
        // if(this.remainingFinishedTasks == 0) throw new RuntimeException("Cannot have below zero remaining tasks.");
        this.remainingFinishedTasks--;
        if(remainingFinishedTasks == 0) moveToDoneState();
    }

    public void signalError(){
        moveToErrorState();
    }

    public long tid() {
        return this.tid;
    }

    public OffsetStatus status(){
        return this.status;
    }
}
