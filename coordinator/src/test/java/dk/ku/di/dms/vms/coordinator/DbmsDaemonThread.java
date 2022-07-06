package dk.ku.di.dms.vms.coordinator;


import dk.ku.di.dms.vms.web_common.runnable.IVMsFutureCancellable;
import dk.ku.di.dms.vms.web_common.runnable.VMSFutureTask;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.random.RandomGenerator;

import static dk.ku.di.dms.vms.coordinator.MessageType.*;

/**
 * Simulation with method calls instead of sockets
 *
 * https://www.confluent.io/blog/configure-kafka-to-minimize-latency/
 */
public class DbmsDaemonThread extends MailBox {

    private final ReplicationStrategyEnum replicationStrategy;

    // other dbms daemons
    private List<DbmsDaemonThread> dbmsList;

    // thread identifier for debugging purposes
    private final int id;

    // starts with zero
    private long lastCommittedOffset = 0;

    private VMSFutureTask<Boolean> checkpointRequest;

    // these have been replicated in at least another dbms
    private List<Message> log;

    public DbmsDaemonThread(int id, List<DbmsDaemonThread> dbmsList){
        this.id = id;
        this.dbmsList = dbmsList;
        this.log = new ArrayList<>();
        this.replicationStrategy = ReplicationStrategyEnum.SYNC; // as default
    }

    /**
     * We need to be sure whether all events from this batch have arrived
     * @param message
     * @return
     */
    public IVMsFutureCancellable<Boolean> checkpoint(Message message){
        Thread currentThread = Thread.currentThread();
        checkpointRequest = new VMSFutureTask<>(currentThread);

//        if(eventsPendingReplication.size() > eventReplicationThresholdForCheckpointingCompleting
//                || remainingEvents() ){
//            Message msg = new Message( CHECKPOINT_COMPLETING, message.payload );
//            message.serverThread.enqueue( msg );
//        } else {
//
//            lastCommittedOffset++;
//            log = new ArrayList<>(); // just believe we are storing it durably
//        }

        return checkpointRequest;

    }

//    private boolean receive(Message message) {
//
//
//        if(replicationStrategy == ReplicationStrategyEnum.SYNC){
//
//            boolean res = replicateSync( message );
//
//
//        }
//    }

    /**
     * @param message the message received
     * @return a future
     */
    public Message replicate(Message message){

        int op = 0; // getRandomResponseForReplicationRequest(random);
        if(op > 0) {
            message.type = REPLICATE_EVENT_ACK;
        } else {
            message.type = REPLICATE_EVENT_NACK;
        }
        return message;

    }

    /**
     * We must implement a strategy for replication.
     * What is the replication factor? Should we replicate the inputs to all DBMS?
     * Let's do strongly consistent
     * What if we cannot replicate to all DBMS?
     * There is another strategy. We make it durable here and asynchronously replicate. We are optimistic
     * What if this dbms crash?
     * @param
     * @param msg
     * @return
     */

    // FIXME tis should be all in the coordinator......
    // the server must safeguard all events are safely replicated

    // i think the design must change... the server must build the batch first?

//    private void replicateAsync(Message msg){
//
//        for(DbmsDaemonThread dbmsThread : dbmsList){
//
//            IVMsFutureCancellable<Boolean> future = dbmsThread.replicate(msg);
//            Boolean res = future.get();
//            if(res != null) {
//                log.add(msg);
//            }
//
//        }
//
//    }

    private boolean replicateSync(Message msg){

        log.add(msg); // now replicate to all

        List<DbmsDaemonThread> dbmsError = new ArrayList<>();

        List<Future<Message>> futures = new ArrayList<>();
        for(DbmsDaemonThread dbmsThread : dbmsList){
            futures.add( executorService.submit( () -> dbmsThread.replicate(msg) ) );
        }

        for(int i = 0; i < futures.size(); i++){
            Future<Message> ft = futures.get(i);
            try {
                ft.get();
            } catch (InterruptedException | ExecutionException ignored) {
                dbmsError.add( dbmsList.get(i) ); // our protocol must wait for this dbms to come back online eventually
            }
        }

        if(dbmsError.size() > 0){

            // enqueue this message for later reattempt

            return false;
        }
        return false;
    }

    @Override
    public void run() {

        RandomGenerator random = RandomGenerator.of("DbmsThread");

        while(isRunning()) {

            if (!queue.isEmpty()) {
                Message msg = queue.peekFirst();

                switch (msg.type) {

                    // async replication request
//                    case REPLICATE_EVENT_REQ -> {
//                        int op = getRandomResponseForReplicationRequest(random);
//                        if(op > 0) {
//                            msg.type = REPLICATE_EVENT_RESP;
//                            msg.dbmsDaemonThread.enqueue(msg);
//                        }
//                    }

                }
            }
        }

    }

    private int getRandomResponseForReplicationRequest(RandomGenerator random){

        int op = random.nextInt(10);

        if (op == 1) {

            // timeout to send later
            try {
                Thread.sleep(random.nextInt(2000, 5000));
            } catch (InterruptedException ignored) {

            }
        }

        return op;

    }

}
