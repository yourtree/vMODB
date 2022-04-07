package dk.ku.di.dms.vms.sdk.core.manager;

import dk.ku.di.dms.vms.sdk.core.event.VmsInternalPubSub;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionScheduler;

import java.util.concurrent.Future;

public class ManagerMetadata {

    public VmsTransactionScheduler scheduler;
    public Future<?> schedulerFuture;

    public boolean initialized;

    public VmsInternalPubSub internalPubSub;
}
