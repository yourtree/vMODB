package dk.ku.di.dms.vms.coordinator;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.random.RandomGenerator;

import static dk.ku.di.dms.vms.coordinator.MessageType.CHECKPOINT;

/**
 * It forwards the transaction request to the DBMS responsible for the respective microservice
 * A transaction might involve more than an input DBMS. In this case, it should be atomic operation
 */
public class ServerThread extends MailBox {

    private List<ServerThread> servers;

    private List<DbmsDaemonThread> dbmsList;

    private AtomicLong offset;

    private LinkedList<Message> logToConfirmCheckpoint;

    private LinkedList<Message> log;

    private boolean checkpointInProgress;

    public ServerThread(){
        this.offset = new AtomicLong(0);
    }

    @Override
    public void run() {

        RandomGenerator random = RandomGenerator.of("ServerThread");

        long startTime = System.currentTimeMillis();
        long checkpointThreshold = 1000; // in case of completing message we increase the threshold to * 2

        // TODO contact f+1 dbms to get the current offset. after a crash this info is lost

        while(isRunning()) {

            if (!queue.isEmpty()) {
                Message msg = queue.peekFirst();

                switch (msg.type) {

                    case EVENT -> {

                        // send event to dbms ... now just pick any
                        int dbmsId = random.nextInt(dbmsList.size());
//                        try {
//                            dbmsList.get(dbmsId).;
//                        } catch (InterruptedException | ExecutionException ignored) {}



                    }

                    // sub batch arriving from another server
                    case SUB_BATCH -> {
                        // for ease fo understanding lets consider a sub batch is composed by one request

                        log.add(msg);

                    }

                    case CHECKPOINT_ACK -> {

                        // increment counter... I have to consider duplicate messages from dbms.. they log before sending but after crashing can send again

                        // when all logToConfirmCheckpoint = null
                        checkpointInProgress = false;
                    }

                    case CHECKPOINT_COMPLETING -> {
                        checkpointThreshold = checkpointThreshold * 2;
                    }


                }

            }

            // if it is time for checkpointing
            if( !checkpointInProgress && System.currentTimeMillis() - startTime >= checkpointThreshold ){
                checkpointInProgress = true;
                // for all input-dbms, send the checkpointing
                logToConfirmCheckpoint = log;
                log = new LinkedList<>();

                for(DbmsDaemonThread dbmsDaemonThread : dbmsList){

                    Message msg = new Message( CHECKPOINT);
                    msg.offset = offset.get() + 1;
                    msg.serverThread = this;

                    // IVMsFutureCancellable<Boolean> fut =
                    dbmsDaemonThread.checkpoint( msg );

                }

            }

        }

    }

    /**
     * I am generating the message with the correct dbms to send....
     * But in real case we would need to verify
     * @param message
     * @return
     */
    private DbmsDaemonThread getDbmsBasedOnTransactionType(Message message){
        return message.dbmsDaemonThread;
    }

}
