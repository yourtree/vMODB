package dk.ku.di.dms.vms.coordinator.server.coordinator.transaction;

import dk.ku.di.dms.vms.coordinator.server.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.server.infra.VmsConnectionMetadata;
import dk.ku.di.dms.vms.coordinator.server.schema.TransactionInput;
import dk.ku.di.dms.vms.coordinator.transaction.EventIdentifier;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.schema.network.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.VmsIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitRequest;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchComplete;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Heartbeat;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.web_common.buffer.BufferManager;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;
import dk.ku.di.dms.vms.web_common.meta.Issue;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static dk.ku.di.dms.vms.coordinator.server.coordinator.options.BatchReplicationStrategy.*;
import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.BATCH_COMPLETE;
import static dk.ku.di.dms.vms.web_common.meta.Constants.BATCH_COMPLETE;
import static dk.ku.di.dms.vms.web_common.meta.Constants.TX_ABORT;
import static dk.ku.di.dms.vms.web_common.meta.Issue.Category.*;
import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.TCP_NODELAY;

/**
 * This task assumes the channels are already established
 * Cannot have two threads writing to the same channel at the same time
 * A transaction manager is responsible for assigning TIDs to incoming transaction requests
 * This task also involves making sure the writes are performed successfully
 * A writer manager is responsible for defining strategies, policies, safety guarantees on
 * writing concurrently to channels.
 *
 * TODO make this the only thread writing to the socket
 *      that requires buffers to read what should be written (for the spawn batch?)
 *      network fluctuation is another problem...
 *      - resend bring here
 *      - process transaction input events here
 *
 */
public class TransactionManager extends StoppableRunnable {

    // in case the VMS fails, we can resend (the inputs only)
    // the internal events must be sent by the other VMSs in case this VMS is in the center of a DAG
    private Map<Long, List<TransactionEvent.Payload>> transactionEventsPerBatch;

    private TransactionManagerContext txManagerCtx;

    public TransactionManager(TransactionManagerContext context){
        this.context = context;
    }

    @Override
    public void run() {

        while(isRunning()){

            try {
                // https://web.mit.edu/6.005/www/fa14/classes/20-queues-locks/message-passing/
                byte action = txManagerCtx.actionQueue().take();

                // do we have any transaction-related event?
                switch(action){
                    case BATCH_COMPLETE -> {

                        // what if ACKs from VMSs take too long? or never arrive?
                        // need to deal with intersecting batches? actually just continue emitting for higher throughput

                        BatchComplete.Payload msg = txManagerCtx.batchCompleteEvents().remove();

                        BatchContext batchContext = batchContextMap.get( msg.batch() );

                        // only if it is not a duplicate vote
                        if( batchContext.missingVotes.remove( msg.vms() ) ){

                            // making this implement order-independent, so not assuming batch commit are received in order, although they are necessarily applied in order both here and in the VMSs
                            // is the current? this approach may miss a batch... so when the batchOffsetPendingCommit finishes, it must check the batch context match to see whether it is completed
                            if( batchContext.batchOffset == batchOffsetPendingCommit && batchContext.missingVotes.size() == 0 ){

                                sendCommitRequestToVMSs(batchContext);

                                // is the next batch completed already?
                                batchContext = batchContextMap.get( ++batchOffsetPendingCommit );
                                while(batchContext.missingVotes.size() == 0){
                                    sendCommitRequestToVMSs(batchContext);
                                    batchContext = batchContextMap.get( ++batchOffsetPendingCommit );
                                }

                            }

                        }


                    }
                    case TX_ABORT -> {

                        // send abort to all VMSs...
                        // later we can optimize the number of messages since some VMSs may not need to receive this abort

                        // cannot commit the batch unless the VMS is sure there will be no aborts...
                        // this is guaranteed by design, since the batch complete won't arrive unless all events of the batch arrive at the terminal VMSs

                        TransactionAbort.Payload msg = txManagerCtx.transactionAbortEvents.remove();

                        // can reuse the same buffer since the message does not change across VMSs like the commit request
                        for(VmsIdentifier vms : VMSs.values()){

                            // don't need to send to the vms that aborted
                            if(vms.getIdentifier().equalsIgnoreCase( msg.vms() )) continue;

                            VmsConnectionMetadata connectionMetadata = vmsConnectionMetadataMap.get( vms.hashCode() );

                            ByteBuffer buffer = BufferManager.loanByteBuffer();

                            TransactionAbort.write(buffer, msg);

                            // must lock first before writing to write buffer
                            connectionMetadata.writeLock.lock();

                            try { connectionMetadata.channel.write(buffer).get(); } catch (ExecutionException ignored) {}

                            connectionMetadata.writeLock.unlock();

                            buffer.clear();

                            BufferManager.returnByteBuffer(buffer);

                        }


                    }
                }
            } catch (InterruptedException e) {
                issueQueue.add( new Issue( TRANSACTION_MANAGER_STOPPED, me ) );
            }

        }

    }

    // this could be asynchronously
    private void sendCommitRequestToVMSs(BatchContext batchContext){

        for(VmsIdentifier vms : VMSs.values()){

            ConnectionMetadata connectionMetadata = vmsConnectionMetadataMap.get( vms.hashCode() );

            // must lock first before writing to write buffer
            connectionMetadata.writeLock.lock();

            // having more than one write buffer does not help us, since the connection is the limitation (one writer at a time)
            BatchCommitRequest.write( connectionMetadata.writeBuffer,
                    batchContext.batchOffset, batchContext.lastTidOfBatchPerVms.get( vms.getIdentifier() ) );

            connectionMetadata.channel.write(connectionMetadata.writeBuffer, connectionMetadata, new Coordinator.WriteCompletionHandler());

        }

    }

    private static final class WriteCompletionHandler0 implements CompletionHandler<Integer, ConnectionMetadata> {

        private int overflowPos;

        public WriteCompletionHandler0(){
            this.overflowPos = 0;
        }

        public WriteCompletionHandler0(int pos){
            this.overflowPos = pos;
        }

        @Override
        public void completed(Integer result, ConnectionMetadata connectionMetadata) {

            if(overflowPos == 0) {
                connectionMetadata.writeBuffer.rewind();
                return;
            }

            int initPos = connectionMetadata.writeBuffer.position() + 1;

            byte[] overflowContent = connectionMetadata.writeBuffer.
                    slice( initPos, overflowPos ).array();

            connectionMetadata.writeBuffer.rewind();

            connectionMetadata.writeBuffer.put( overflowContent );

        }

        /**
         * If failed, probably the VMS is down.
         * When getting back, the resend task will write data to the buffer again
         */
        @Override
        public void failed(Throwable exc, ConnectionMetadata connectionMetadata) {
            connectionMetadata.writeBuffer.rewind();
        }

    }

    /**
     * Allows to reuse the thread pool assigned to socket to complete the writing
     * That refrains the main thread and the TransactionManager to block, thus allowing its progress
     */
    private static final class WriteCompletionHandler implements CompletionHandler<Integer, ConnectionMetadata> {

        @Override
        public void completed(Integer result, ConnectionMetadata connectionMetadata) {
            connectionMetadata.writeBuffer.rewind();
            connectionMetadata.writeLock.unlock();
        }

        @Override
        public void failed(Throwable exc, ConnectionMetadata connectionMetadata) {
            connectionMetadata.writeBuffer.rewind();
            connectionMetadata.writeLock.unlock();
        }

    }


    /**
     *  Should read in a proportion that matches the batch and heartbeat window, otherwise
     *  how long does it take to process a batch of input transactions?
     *  instead of trying to match the rate of processing, perhaps we can create read tasks
     *
     * TODO do not send the transactions. the batch commit should perform this
     *  only parse then and save in memory data structure
     *
     *  processing the transaction input and creating the
     *  corresponding events
     *  the network buffering will send
     */
    private void processTransactionInputEvents(){

        int size = context.parsedTransactionRequests.size();
        if( parsedTransactionRequests.size() == 0 ){
            return;
        }

        Collection<TransactionInput> transactionRequests = new ArrayList<>(size + 50); // threshold, for concurrent appends
        parsedTransactionRequests.drainTo( transactionRequests );

        for(TransactionInput transactionInput : transactionRequests){

            // this is the only thread updating this value, so it is by design an atomic operation
            long tid_ = ++tid;

            TransactionDAG transactionDAG = transactionMap.get( transactionInput.name );

            // should find a way to continue emitting new transactions without stopping this thread
            // non-blocking design
            // having the batch here guarantees that all input events of the same tid does
            // not belong to different batches

            // to allow for a snapshot of the last TIDs of each vms involved in this transaction
            long batch_;
            synchronized (batchCommitLock) {
                // get the batch here since the batch handler is also incrementing it inside a synchronized block
                batch_ = batchOffset;
            }

            // for each input event, send the event to the proper vms
            // assuming the input is correct, i.e., all events are present
            for (TransactionInput.Event inputEvent : transactionInput.events) {

                // look for the event in the topology
                EventIdentifier event = transactionDAG.topology.get(inputEvent.name);

                // get the vms
                VmsIdentifier vms = VMSs.get(event.vms.hashCode());

                // get the connection metadata
                VmsConnectionMetadata connectionMetadata = vmsConnectionMetadataMap.get( vms.hashCode() );

                // we could employ deterministic writes to the channel, that is, an order that would not require locking for writes
                // we could possibly open a channel per write operation, but... what are the consequences of too much opened connections?
                // FIXME make this thread handles the resend, this way we don't need locks
                connectionMetadata.writeLock.lock();

                // get current pos to later verify whether
                // int currentPos = connectionMetadata.writeBuffer.position();
                connectionMetadata.writeBuffer.mark();

                // write. think about failures/atomicity later
                TransactionEvent.Payload txEvent = TransactionEvent.write(connectionMetadata.writeBuffer, tid_, vms.lastTid, batch_, inputEvent.name, inputEvent.payload);

                // always assuming the buffer capacity is able to sustain the next write...
                if(connectionMetadata.writeBuffer.position() > batchBufferSize){
                    int overflowPos = connectionMetadata.writeBuffer.position();

                    // return to marked position, so the overflow content is not written
                    connectionMetadata.writeBuffer.reset();

                    connectionMetadata.channel.write(connectionMetadata.writeBuffer,
                            connectionMetadata, new Coordinator.WriteCompletionHandler0(overflowPos));

                }

                // assign this event, so... what? try to send later? if a vms fail, the last event is useless, we need to send the whole batch generated so far...
                List<TransactionEvent.Payload> list = connectionMetadata.transactionEventsPerBatch.get(batch_);
                if (list == null ){
                    connectionMetadata.transactionEventsPerBatch.put(batch_, new ArrayList<>());
                } else {
                    list.add(txEvent);
                }

                // a vms, although receiving an event from a "next" batch, cannot yet commit, since
                // there may have additional events to arrive from the current batch
                // so the batch request must contain the last tid of the given vms

                // update for next transaction
                vms.lastTid = tid_;

            }

            // add terminal to the set... so cannot be immutable when the batch context is created...
            batchContextMap.get(batch_).terminalVMSs.addAll( transactionDAG.terminals );

        }

    }

    /**
     * (a) Heartbeat sending to avoid followers to initiate a leader election. That can still happen due to network latency.
     *
     * Given a list of known followers, send to each a heartbeat
     * Heartbeats must have precedence over other writes, since they
     * avoid the overhead of starting a new election process in remote nodes
     * and generating new messages over the network.
     *
     * I can implement later a priority-based scheduling of writes.... maybe some Java DT can help?
     */
    private void sendHeartbeats() {
        logger.info("Sending vote requests. I am "+ me.host+":"+me.port);
        for(ServerIdentifier server : servers.values()){
            ConnectionMetadata connectionMetadata = serverConnectionMetadataMap.get( server.hashCode() );
            if(connectionMetadata.channel != null) {
                Heartbeat.write(connectionMetadata.writeBuffer, me);
                connectionMetadata.writeLock.lock();
                connectionMetadata.channel.write( connectionMetadata.writeBuffer, connectionMetadata, new WriteCompletionHandler() );
            } else {
                issueQueue.add( new Issue( CHANNEL_NOT_REGISTERED, server ) );
            }
        }
    }

    /**
     * A thread will execute this piece of code to liberate the "Accept" thread handler
     * This only works for input events. In case of internal events, the VMS needs to get that from the precedence VMS.
     */
    private void resendTransactionalInputEvents(VmsConnectionMetadata connectionMetadata, VmsIdentifier vmsIdentifier){

        // is this usually the last batch...?
        List<TransactionEvent.Payload> list = connectionMetadata.transactionEventsPerBatch.get( vmsIdentifier.lastBatch + 1 );

        // assuming everything is lost, we have to resend...
        if( list != null ){
            connectionMetadata.writeLock.lock();
            for( TransactionEvent.Payload txEvent : list ){
                TransactionEvent.write( connectionMetadata.writeBuffer, txEvent);
                Future<?> task = connectionMetadata.channel.write( connectionMetadata.writeBuffer );
                try { task.get(); } catch (InterruptedException | ExecutionException ignored) {}
                connectionMetadata.writeBuffer.clear();
            }
        }

    }

    /**
     * Given a set of VMSs involved in the last batch
     * (for easiness can send to all of them for now)
     * send a batch request.
     *
     * Callback to start batch commit process
     *
     * TODO store the transactions in disk before sending
     */
    private void spawnBatchCommit(){

        // we need a cut. all vms must be aligned in terms of tid
        // because the other thread might still be sending intersecting TIDs
        // e.g., vms 1 receives batch 1 tid 1 vms 2 received batch 2 tid 1
        // this is solved by fixing the batch per transaction (and all its events)
        // this is an anomaly across batches

        // but another problem is that an interleaving can allow the following problem
        // commit handler issues batch 1 to vms 1 with last tid 1
        // but tx-mgr issues event with batch 1 vms 1 last tid 2
        // this can happen because the tx-mgr is still in the loop
        // this is an anomaly within a batch

        // so we need synchronization to disallow the second

        // obtain a consistent snapshot of last TIDs for all VMSs
        // the transaction manager must obtain the next batch inside the synchronized block

        // why do I need to replicate vmsTidMap? to restart from this point if the leader fails
        Map<String,Long> lastTidOfBatchPerVms;
        long currBatch;
        synchronized (batchCommitLock) {

            currBatch = batchOffset;

            BatchContext currBatchContext = batchContextMap.get( currBatch );
            currBatchContext.seal();

            // a map of the last tid for each vms
            lastTidOfBatchPerVms = VMSs.values().stream().collect(
                    Collectors.toMap( VmsIdentifier::getIdentifier, VmsIdentifier::getLastTid ) );

            // define new batch context
            // need to get
            BatchContext newBatchContext = new BatchContext(batchOffset, lastTidOfBatchPerVms);
            batchContextMap.put( batchOffset, newBatchContext );

            long newBatch = ++batchOffset; // first increase the value and then execute the statement

            logger.info("Current batch offset is "+currBatch+" and new batch offset is "+newBatch);

        }
        // new TIDs will be emitted with the new batch in the transaction manager

        // to refrain the number of servers increasing concurrently, instead of
        // synchronizing the operation, I can simply obtain the collection first
        // but what happens if one of the servers in the list fails?
        Collection<ServerIdentifier> activeServers = servers.values();
        int nServers = activeServers.size();

        CompletableFuture<?>[] promises = new CompletableFuture[nServers];

        Set<Integer> serverVotes = Collections.synchronizedSet( new HashSet<>(nServers) );

        String lastTidOfBatchPerVmsJson = serdesProxy.serializeMap( lastTidOfBatchPerVms );

        int i = 0;
        for(ServerIdentifier server : activeServers){

            if(!server.isActive()) continue;
            promises[i] = CompletableFuture.supplyAsync( () ->
            {
                // could potentially use another channel for writing commit-related messages...
                // could also just open and close a new connection
                // actually I need this since I must read from this thread instead of relying on the
                // read completion handler
                AsynchronousSocketChannel channel = null;
                try {

                    InetSocketAddress address = new InetSocketAddress(server.host, server.port);
                    channel = AsynchronousSocketChannel.open(group);
                    channel.setOption( TCP_NODELAY, true );
                    channel.setOption( SO_KEEPALIVE, false );
                    channel.connect(address).get();

                    ByteBuffer buffer = BufferManager.loanByteBuffer();
                    BatchReplication.write( buffer, currBatch, lastTidOfBatchPerVmsJson );
                    channel.write( buffer ).get();

                    buffer.clear();

                    // immediate read in the same channel
                    channel.read( buffer ).get();

                    BatchReplication.BatchReplicationPayload response = BatchReplication.read( buffer );

                    buffer.clear();

                    BufferManager.returnByteBuffer(buffer);

                    // assuming the follower always accept
                    if(currBatch == response.batch()) serverVotes.add( server.hashCode() );

                    return null;

                } catch (InterruptedException | ExecutionException | IOException e) {
                    // cannot connect to host
                    logger.warning("Error connecting to host. I am "+ me.host+":"+me.port+" and the target is "+ server.host+":"+ server.port);
                    return null;
                } finally {
                    if(channel != null && channel.isOpen()) {
                        try {
                            channel.close();
                        } catch (IOException ignored) {}
                    }
                }

                // these threads need to block to wait for server response

            }, taskExecutor).exceptionallyAsync( (x) -> {
                defaultLogError( UNREACHABLE_NODE, server );
                return null;
            }, taskExecutor);
            i++;
        }

        if ( batchReplicationStrategy == ONE ){
            // asynchronous
            // at least one is always necessary
            int j = 0;
            while (j < nServers && serverVotes.size() < 1){
                promises[i].join();
                j++;
            }
            if(serverVotes.isEmpty()){
                logger.warning("The system has entered in a state that data may be lost since there are no followers to replicate the current batch offset.");
            }
        } else if ( batchReplicationStrategy == MAJORITY ){

            int simpleMajority = ((nServers + 1) / 2);
            // idea is to iterate through servers, "joining" them until we have enough
            int j = 0;
            while (j < nServers && serverVotes.size() <= simpleMajority){
                promises[i].join();
                j++;
            }

            if(serverVotes.size() < simpleMajority){
                logger.warning("The system has entered in a state that data may be lost since a majority have not been obtained to replicate the current batch offset.");
            }
        } else if ( batchReplicationStrategy == ALL ) {
            CompletableFuture.allOf( promises ).join();
            if ( serverVotes.size() < nServers ) {
                logger.warning("The system has entered in a state that data may be lost since there are missing votes to replicate the current batch offset.");
            }
        }

        // for now, we don't have a fallback strategy...

    }


}