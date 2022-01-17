package dk.ku.di.dms.vms.manager;

import dk.ku.di.dms.vms.event.EventRepository;
import dk.ku.di.dms.vms.event.IEventHandler;
import dk.ku.di.dms.vms.operational.DataOperationExecutor;
import dk.ku.di.dms.vms.scheduler.Scheduler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ManagerMetadata {

    public Scheduler scheduler;
    public Future schedulerFuture;

    public IEventHandler eventHandler;
    public Future eventHandlerFuture;

    public DataOperationExecutor executor;
    public Future executorFuture;

    public ExecutorService executorService;

    public boolean initialized = false;

    public EventRepository eventRepository;
}
