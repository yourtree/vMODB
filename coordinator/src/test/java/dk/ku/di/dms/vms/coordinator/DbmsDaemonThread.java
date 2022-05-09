package dk.ku.di.dms.vms.coordinator;


import dk.ku.di.dms.vms.modb.common.interfaces.IVmsFuture;
import dk.ku.di.dms.vms.web_common.runnable.IVMsFutureCancellable;
import dk.ku.di.dms.vms.web_common.runnable.VMSFutureTask;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.random.RandomGenerator;

import static dk.ku.di.dms.vms.coordinator.MessageType.CHECKPOINT_COMPLETING;

/**
 * Simulation with method calls instead of sockets
 */
public class DbmsDaemonThread extends MailBox {

    // other dbms daemons
    private List<DbmsDaemonThread> dbmsList;

    // thread identifier for debugging purposes
    private final int id;

    private long lastCommittedOffset;

    private VMSFutureTask<Boolean> checkpointRequest;

    // these have been replicated in at least another dbms
    private final List<Message> eventsReadyForCommit;

    private final List<Message> eventsPendingReplication;

    private final ConcurrentLinkedDeque<VMSFutureTask<Boolean>> replicateRequests;

    private long threshold = 10000;

    public DbmsDaemonThread(int id, List<DbmsDaemonThread> dbmsList){
        this.id = id;
        this.dbmsList = dbmsList;
        this.replicateRequests = new ConcurrentLinkedDeque<>();

        this.eventsReadyForCommit = new ArrayList<>();
        this.eventsPendingReplication = new ArrayList<>();
    }

    public IVMsFutureCancellable<Boolean> checkpoint(Message message){
        Thread currentThread = Thread.currentThread();
        checkpointRequest = new VMSFutureTask<Boolean>(currentThread);

        if(eventsPendingReplication.size() > 0){
            Message msg = new Message( CHECKPOINT_COMPLETING, message.payload );
            message.serverThread.queue( msg );
        }

        return checkpointRequest;

    }

    /**
     * For this example we can simply discard the msg...
     * The idea is that these events are in memory prior to checkpoint
     * @param message the message received
     * @return a future
     */
    public IVMsFutureCancellable<Boolean> replicate(Message message){
        Thread currentThread = Thread.currentThread();
        VMSFutureTask<Boolean> futureTask = new VMSFutureTask<Boolean>(currentThread);
        replicateRequests.add(futureTask);
        return futureTask;
    }

    private boolean tryToReplicate(RandomGenerator random, Message msg){
        // replicate... for ease of understanding, pick one
        int dbmsId = random.nextInt(dbmsList.size());
        IVMsFutureCancellable<Boolean> future = dbmsList.get(dbmsId).replicate(msg);
        Boolean res = future.get(1000);
        if(res != null) {
            eventsReadyForCommit.add(msg);
            return true;
        }
        return false;
    }

    @Override
    public void run() {

        RandomGenerator random = RandomGenerator.of("DbmsThread");

        while(!isStopped()){

            if(!queue.isEmpty()) {
                Message msg = queue.peekFirst();

                switch(msg.type){

                    // events, does not matter the order of processing, as long as all in the batch are processed
                    case EVENT -> {

                        if(!tryToReplicate(random, msg)){
                            msg.timestamp = System.currentTimeMillis();
                            eventsPendingReplication.add(msg);
                        }


                        // what should be done? can either try other dbms or respond the server accordingly

                        // let's try one more time
//                        dbmsId = random.nextInt(dbmsList.size());
//                        future = dbmsList.get(dbmsId).replicate(msg);
//                        res = future.get();

                        // another strategy is not sending acks... on the checkpoint we can deal with it, the server coord will increase the batch size...
//                        if(res != null) {
//                            msg.serverThread.queue( new Message(MessageType.EVENT_ACK, msg) );
//                        } else {
//                            // lets try to tell the server to hold a bit until enough dbmss come back from failure
//                            msg.serverThread.queue( new Message(MessageType.EVENT_NACK, msg) );
//                        }

                    }

                }

            }

            // deal with event replication requests -- synchronous
            if(!replicateRequests.isEmpty()){

                final VMSFutureTask<Boolean> request = replicateRequests.peekFirst();

                int op = random.nextInt(6);
                if(op == 0){
                    // discard event to force requester to look for other dbms
                    request.cancel();
                } if(op == 1) {

//                    Timer timer = new Timer(true);
//                    TimerTask task = new TimerTask() {
//                        @Override
//                        public void run() {
//                            request.set(true);
//                        }
//                    };
//                    timer.schedule(task, 2*60*1000);

                    // timeout to send later
                    try {
                        Thread.sleep(random.nextInt(2000,5000));
                    } catch (InterruptedException e) {
                        // e.printStackTrace();
                    } finally {
                        request.set(true);
                    }
                } else { // 2,3,4,5 75% of change of replicating it
                    request.set(true); // responding true
                }

            }

            long now = System.currentTimeMillis();
            if(!eventsPendingReplication.isEmpty() && now - eventsPendingReplication.get(0).timestamp >= threshold ) {
                // let's try to replicate again
                if( tryToReplicate(random, eventsPendingReplication.get(0)) ){
                    eventsPendingReplication.remove(0);
                }
            }

        }

    }

}
