package dk.ku.di.dms.vms.sdk.core.manager;

import dk.ku.di.dms.vms.sdk.core.event.handler.VmsEventHandler;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionScheduler;

/**
 * The runtime (sdk) metadata
 */
class ManagerMetadata {

    public VmsTransactionScheduler scheduler;
    public Thread schedulerThread;

    public VmsEventHandler eventHandler;
    public Thread eventHandlerThread;
}
