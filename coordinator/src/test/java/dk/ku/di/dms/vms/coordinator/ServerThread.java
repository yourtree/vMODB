package dk.ku.di.dms.vms.coordinator;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.random.RandomGenerator;

import static dk.ku.di.dms.vms.coordinator.MessageType.CHECKPOINT;

public class ServerThread extends MailBox {

    private List<ServerThread> servers;

    private List<DbmsDaemonThread> dbmsList;

    private AtomicLong offset;

    private LinkedList<Message> logToConfirmCheckpoint;

    private LinkedList<Message> currLog;

    private boolean checkpointInProgress;

    @Override
    public void run() {

        RandomGenerator random = RandomGenerator.of("ServerThread");

        long startTime = System.currentTimeMillis();
        long checkpointThreshold = 1000; // in case of completing message we increase the threshold to * 2

        while(!isStopped()) {

            if (!queue.isEmpty()) {
                Message msg = queue.peekFirst();

                switch (msg.type) {

                    case EVENT -> {

                        // send event to dbms ... now just pick any
                        int dbmsId = random.nextInt(dbmsList.size());
                        try {
                            dbmsList.get(dbmsId).queue(msg).get();
                        } catch (InterruptedException | ExecutionException ignored) {}



                    }

                    // sub batch arriving from another server
                    case SUB_BATCH -> {
                        // for ease fo understanding lets consider a sub batch is composed by one request

                        currLog.add(msg);

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
                logToConfirmCheckpoint = currLog;
                currLog = new LinkedList<>();

                for(DbmsDaemonThread dbmsDaemonThread : dbmsList){

                    Message msg = new Message( CHECKPOINT, offset.get() + 1 );
                    msg.serverThread = this;

                    // IVMsFutureCancellable<Boolean> fut =
                    dbmsDaemonThread.checkpoint( msg );

                }


            }



        }

    }

}
