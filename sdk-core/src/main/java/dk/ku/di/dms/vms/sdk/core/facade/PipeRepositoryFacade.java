package dk.ku.di.dms.vms.sdk.core.facade;

import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchService;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static java.nio.file.StandardWatchEventKinds.*;

/**
 * how many pipes are necessary?
 *
 * one for database requests
 * one for database responses
 * one for input events
 * one for output events
 *
 * By using pipes, we can expose the direct memory address to the caller
 * thus avoiding (useless) data transfer since both processes are on the
 * same machine
 *
 * But pipe has fifo semantics. the response must be matched with the caller
 * Since multiple threads may waiting for the queue, this can interleave
 * We need a another agent to match requests against responses.
 *
 *
 *
 */
public class PipeRepositoryFacade implements IVmsRepositoryFacade, InvocationHandler {

    private final Class<?> pkClazz;

    private final Class<? extends IEntity<?>> entityClazz;

    @SuppressWarnings({"unchecked"})
    public PipeRepositoryFacade(Class<? extends IRepository<?,?>> repositoryClazz) throws IOException {

        Type[] types = ((ParameterizedType) repositoryClazz.getGenericInterfaces()[0]).getActualTypeArguments();

        this.entityClazz = (Class<? extends IEntity<?>>) types[1];
        this.pkClazz = (Class<?>) types[0];

        WatchService watcher = FileSystems.getDefault().newWatchService();

        WatchEvent.Kind<?>[] events = new WatchEvent.Kind[]{
                ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY
        };

        Path dbmsRequests = Path.of("./DBMS-OUT/dbmsRequests");
        Path dbmsResponses = Path.of("./DBMS-IN/dbmsResponses");
        dbmsResponses.register(watcher, events);

        Path outputEvents = Path.of("./OUT/outputEvents");
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        return null;
    }


    @Override
    public Object fetch(SelectStatement selectStatement, Type type) {
        return null;
    }

    @Override
    public Object fetch(SelectStatement selectStatement) {
        return null;
    }

    @Override
    public void insertAll(List<Object> entities) {

    }

    @Override
    public InvocationHandler asInvocationHandler() {
        return this;
    }

}
