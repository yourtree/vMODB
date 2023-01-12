package dk.ku.di.dms.vms.sdk.core.scheduler;

import java.util.concurrent.Future;

public interface ISchedulerHandler {

    /**
     * Ideally should run in a caller's thread
     * @return nothing
     */
    Future<?> run();

    boolean conditionHolds();

}
