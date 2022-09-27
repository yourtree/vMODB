package dk.ku.di.dms.vms.sdk.core.operational;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * The context of the tasks from a transaction
 * (thus having the same TID)
 * in a single VMS.
 *
 * The tasks found in the lists are READY to be scheduled for execution.
 * In other words, all input are fulfilled.
 */
public class VmsTransactionContext {

    // R
    public final List<VmsTransactionTask> readTasks;

    // RW
    public final List<VmsTransactionTask> readWriteTasks;

    // W
    public final List<VmsTransactionTask> writeTasks;

    public final List<Future<VmsTransactionTaskResult>> submittedTasks;

    public final List<VmsTransactionTaskResult> resultTasks;

    public VmsTransactionContext(int numReadTasks, int numReadWriteTasks, int numWriteTasks) {
        this.readTasks = new ArrayList<>(numReadTasks);
        //this.readSubmitted = new boolean[numReadTasks];
        this.readWriteTasks = new ArrayList<>(numReadWriteTasks);
        //this.readWriteSubmitted = new boolean[numReadWriteTasks];
        this.writeTasks = new ArrayList<>(numWriteTasks);
        //this.writeSubmitted = new boolean[numWriteTasks];
        int total = numReadTasks + numWriteTasks + numWriteTasks;
        this.submittedTasks = new ArrayList<>(total);
        this.resultTasks = new ArrayList<>(total);
    }
}
