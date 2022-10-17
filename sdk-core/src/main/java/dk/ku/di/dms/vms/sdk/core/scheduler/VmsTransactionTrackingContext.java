package dk.ku.di.dms.vms.sdk.core.scheduler;

import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTask;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionTaskResult;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Future;

/**
 * The context of the tasks from a transaction
 * (thus having the same TID)
 * in a single VMS.
 *
 * The tasks found in the lists are READY to be scheduled for execution.
 * In other words, all inputs are fulfilled.
 */
class VmsTransactionTrackingContext {

    private int nextTaskIdentifier;

    // R
    public final List<VmsTransactionTask> readTasks;

    // RW
    public final Queue<VmsTransactionTask> readWriteTasks;

    // W
    public final List<VmsTransactionTask> writeTasks;

    public final List<Future<VmsTransactionTaskResult>> submittedTasks;

    public final List<VmsTransactionTaskResult> resultTasks;

    public VmsTransactionTrackingContext(int numReadTasks, int numReadWriteTasks, int numWriteTasks) {
        this.nextTaskIdentifier = 1;
        this.readTasks = new ArrayList<>(numReadTasks);
        //this.readSubmitted = new boolean[numReadTasks];
        this.readWriteTasks = new ArrayDeque<>(numReadWriteTasks);
        //this.readWriteSubmitted = new boolean[numReadWriteTasks];
        this.writeTasks = new ArrayList<>(numWriteTasks);
        //this.writeSubmitted = new boolean[numWriteTasks];
        int total = numReadTasks + numWriteTasks + numWriteTasks;
        this.submittedTasks = new ArrayList<>(total);
        this.resultTasks = new ArrayList<>(total);
    }

    public int getNextTaskIdentifier(){
        return nextTaskIdentifier++;
    }

}
