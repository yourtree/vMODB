package dk.ku.di.dms.vms.coordinator;

import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.web_common.runnable.VmsDaemonThreadFactory;

import java.util.concurrent.*;

/**
 * TODO Implement leader election here?
 */
public abstract class MailBox extends StoppableRunnable {

    protected final ExecutorService executorService;

    protected final ConcurrentLinkedDeque<Message> queue;

    public MailBox(){
        executorService = Executors.newFixedThreadPool( 2, new VmsDaemonThreadFactory());
        queue = new ConcurrentLinkedDeque<>();
    }

    public Future<Boolean> enqueue(Message msg){
         return executorService.submit( new PublisherThread( msg ) );
    }

    private class PublisherThread implements Callable<Boolean> {

        private Message msg;

        public PublisherThread(Message msg){
            this.msg = msg;
        }

        @Override
        public Boolean call() {
            return queue.add( msg );
        }
    }


}
