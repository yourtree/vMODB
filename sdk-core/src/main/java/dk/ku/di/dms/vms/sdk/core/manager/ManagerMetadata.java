package dk.ku.di.dms.vms.sdk.core.manager;

import dk.ku.di.dms.vms.sdk.core.event.EventChannel;
import dk.ku.di.dms.vms.sdk.core.event.IEventHandler;
import dk.ku.di.dms.vms.sdk.core.scheduler.Scheduler;
import dk.ku.di.dms.vms.sdk.core.operational.VmsTransactionExecutor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ManagerMetadata {

    public Scheduler scheduler;
    public Future<?> schedulerFuture;

    public IEventHandler eventHandler;
    public Future<?> eventHandlerFuture;

    public VmsTransactionExecutor executor;
    public Future<?> executorFuture;

    public ExecutorService executorService;

    public boolean initialized;

    public EventChannel eventChannel;
}
