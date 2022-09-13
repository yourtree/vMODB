package dk.ku.di.dms.vms.sdk.embed;

import dk.ku.di.dms.vms.sdk.core.event.handler.IVmsEventHandler;
import dk.ku.di.dms.vms.sdk.core.event.channel.IVmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.web_common.buffer.BufferManager;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;
import dk.ku.di.dms.vms.web_common.meta.Issue;
import dk.ku.di.dms.vms.modb.common.schema.network.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.VmsIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchAbortRequest;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitRequest;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.*;
import static dk.ku.di.dms.vms.web_common.meta.Issue.Category.CANNOT_CONNECT_TO_NODE;
import static dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation.SERVER_TYPE;
import static dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation.YES;
import static java.net.StandardSocketOptions.*;

/**
 * This default event handler connects direct to the coordinator
 * So in this approach it bypasses the sidecar. In this way,
 * the DBMS must also be run within this code.
 *
 * The virtual microservice don't know who is the coordinator. It should be passive.
 * The leader and followers must share a list of VMSs.
 * Could also try to adapt to JNI:
 * https://nachtimwald.com/2017/06/17/calling-java-from-c/
 */
public final class EmbedVmsEventHandler extends SignalingStoppableRunnable implements IVmsEventHandler {

    /** EXECUTOR SERVICE (for socket channels) **/
    private final ExecutorService taskExecutor;

    /** SERVER SOCKET **/
    // other VMSs may want to connect in order to send events
    private final AsynchronousServerSocketChannel serverSocket;

    private final AsynchronousChannelGroup group;

    /** INTERNAL CHANNELS **/
    private final IVmsInternalChannels vmsInternalChannels;

    /** VMS METADATA **/
    private final VmsIdentifier me; // this merges network and semantic data about the vms
    private final VmsRuntimeMetadata vmsMetadata;

    /** EXTERNAL VMSs **/
    private List<VmsIdentifier> consumerVms;
    private Map<Integer, ConnectionMetadata> vmsConnectionMetadataMap;

    /** SERIALIZATION **/
    private final IVmsSerdesProxy serdesProxy;

    /** COORDINATOR **/
    private ServerIdentifier leader;
    private ConnectionMetadata leaderConnectionMetadata;

    /** INTERNAL STATE **/
    private long currentTid;
    private long currentBatchOffset;

    public EmbedVmsEventHandler(IVmsInternalChannels vmsInternalChannels, // for communicating with other components
                                VmsIdentifier me, // to identify which vms this is
                                VmsRuntimeMetadata vmsMetadata, // metadata about this vms
                                IVmsSerdesProxy serdesProxy, // ser/des of objects
                                ExecutorService executorService, // for recurrent and continuous tasks
                                AsynchronousServerSocketChannel serverSocket,
                                AsynchronousChannelGroup group) {
        super();

        this.vmsInternalChannels = vmsInternalChannels;
        this.me = me;
        this.vmsMetadata = vmsMetadata;

        this.serdesProxy = serdesProxy;
        this.taskExecutor = executorService;

        this.serverSocket = serverSocket;
        this.group = group;
    }

    @Override
    public void run() {

        // we must also load internal state
        loadInternalState();

        // setup accept since we need to accept connections from the coordinator and other VMSs
        serverSocket.accept( null, new AcceptCompletionHandler());

        // while not stopped
        while(isRunning()){

            //  write events to leader and VMSs...

            try {

                issueQueue.take();

            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

    }

    /**
     * A thread that basically writes events to other VMSs and the Leader
     * Retrieves data from all output queues
     *
     * All output queues must be read in order to send their data
     *
     * A batch strategy for sending would involve sleeping until the next timeout for batch,
     * send and set up the next. Do that iteratively
     */
    private class WriterThread extends StoppableRunnable {

        @Override
        public void run() {

            BlockingQueue<byte> actionQueue = vmsInternalChannels.actionQueue();

            while (isRunning()){

                try {
                    byte action = actionQueue.take();

                    switch (action){

                        case BATCH_COMPLETE -> {
                            // batchCompleteQueue
                        }

                        case TX_ABORT -> {
                            // transactionAbortOutputQueue
                        }

                        case EVENT -> {
                            // transactionOutputQueue
                        }

                    }



                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }

        }
    }

    /**
     * Last committed state is read on startup. More to come...
     */
    private void loadInternalState() {
        this.currentTid = 0;
        this.currentBatchOffset = 0;
    }

    /**
     * The leader will let each VMS aware of their dependencies,
     * to which VMSs they have to connect to
     */
    private void connectToConsumerVMSs() {

        for(VmsIdentifier vms : consumerVms) {

            try {

                InetSocketAddress address = new InetSocketAddress(vms.host, vms.port);
                AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(group);
                // withDefaultSocketOptions( channel );

                channel.connect(address).get();

                ConnectionMetadata connMetadata = new ConnectionMetadata(vms.hashCode(),
                        ConnectionMetadata.NodeType.VMS,
                        BufferManager.loanByteBuffer(),
                        BufferManager.loanByteBuffer(), channel, new ReentrantLock());

                vmsConnectionMetadataMap.put(vms.hashCode(), connMetadata);

                String dataSchema = serdesProxy.serializeDataSchema( this.me.dataSchema );
                String eventSchema = serdesProxy.serializeEventSchema( this.me.eventSchema );

                Presentation.writeVms( connMetadata.writeBuffer, me, dataSchema, eventSchema );

                channel.write( connMetadata.writeBuffer );

                channel.read(connMetadata.readBuffer, connMetadata, new VmsReadCompletionHandler() );

            } catch (IOException | ExecutionException | InterruptedException e) {
                issueQueue.add( new Issue(CANNOT_CONNECT_TO_NODE, vms.hashCode()) );
            }

        }
    }

    private class VmsReadCompletionHandler implements CompletionHandler<Integer, ConnectionMetadata> {

        @Override
        public void completed(Integer result, ConnectionMetadata connectionMetadata) {

            // can only be event
            connectionMetadata.readBuffer.position(1);

            // data dependence or input event
            TransactionEvent.Payload transactionEventPayload = TransactionEvent.read(connectionMetadata.readBuffer);

            connectionMetadata.readBuffer.clear();

            // send to scheduler
            if(vmsMetadata.queueToEventMap().get( transactionEventPayload.event() ) != null) {
                vmsInternalChannels.transactionInputQueue().add(transactionEventPayload);
            }

            connectionMetadata.channel.read(connectionMetadata.readBuffer, connectionMetadata, this);

        }

        @Override
        public void failed(Throwable exc, ConnectionMetadata connectionMetadata) {
            if (connectionMetadata.channel.isOpen()){
                connectionMetadata.channel.read(connectionMetadata.readBuffer, connectionMetadata, this);
            }
        }
    }

    private class AcceptCompletionHandler implements CompletionHandler<AsynchronousSocketChannel, Void> {

        @Override
        public void completed(AsynchronousSocketChannel channel, Void void_) {

            final ByteBuffer buffer = BufferManager.loanByteBuffer();

            try {

                channel.setOption(TCP_NODELAY, true);
                channel.setOption(SO_KEEPALIVE, true);

                // read presentation message. if vms, receive metadata, if follower, nothing necessary
                Future<Integer> readFuture = channel.read( buffer );
                readFuture.get();

                // release the socket thread pool
                taskExecutor.submit( () -> processAcceptConnection(channel, buffer));

            } catch(Exception ignored){ } finally {
                // continue listening
                if (serverSocket.isOpen()){
                    serverSocket.accept(null, this);
                }
            }

        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            if (serverSocket.isOpen()){
                serverSocket.accept(null, this);
            }
        }

    }

    private void processAcceptConnection(AsynchronousSocketChannel channel, ByteBuffer readBuffer) {

        // message identifier
        byte messageIdentifier = readBuffer.get(0);

        // for now let's consider only the leader connects to the vms
        if(messageIdentifier == SERVER_TYPE){

            boolean includeMetadata = readBuffer.get() == YES;

            // leader has disconnected, or new leader
            Presentation.PayloadFromServer payloadFromServer = Presentation.readServer(readBuffer, serdesProxy);

            // only connects to all VMSs on first leader connection
            boolean connectToVMSs = false;
            if(leader == null || !leader.isActive()) {
                connectToVMSs = true;
                this.consumerVms = payloadFromServer.consumers();

                ByteBuffer writeBuffer = BufferManager.loanByteBuffer();

                readBuffer.clear();

                if(includeMetadata) {
                    String vmsDataSchemaStr = serdesProxy.serializeDataSchema(me.dataSchema);
                    String vmsEventSchemaStr = serdesProxy.serializeEventSchema(me.eventSchema);
                    Presentation.writeVms(writeBuffer, me, vmsDataSchemaStr, vmsEventSchemaStr);

                    try {
                        channel.write( writeBuffer ).get();
                        writeBuffer.clear();
                    } catch (InterruptedException | ExecutionException ignored) {

                        if(!channel.isOpen()) {
                            leader.off();

                            // return buffers
                            BufferManager.returnByteBuffer(readBuffer);
                            BufferManager.returnByteBuffer(writeBuffer);

                            return;
                        }
                        // else what to do try again?

                    }

                }

                // then setup connection metadata and read completion handler
                leaderConnectionMetadata = new ConnectionMetadata(
                        leader.hashCode(),
                        ConnectionMetadata.NodeType.SERVER,
                        readBuffer,
                        writeBuffer,
                        channel,
                        new ReentrantLock()
                );

            } else {
                this.leader = payloadFromServer.serverIdentifier(); // this will set on again
                leaderConnectionMetadata.channel = channel; // update channel
            }

            channel.read(readBuffer, leaderConnectionMetadata, new LeaderReadCompletionHandler() );

            if(connectToVMSs)
                connectToConsumerVMSs();

        } else {
            // then it is a vms intending to connect due to a data/event
            // that should be delivered to this vms

            VmsIdentifier newVms = Presentation.readVms(readBuffer, serdesProxy);

            readBuffer.clear();

            ByteBuffer writeBuffer = BufferManager.loanByteBuffer();

            ConnectionMetadata connMetadata = new ConnectionMetadata(
                    leader.hashCode(),
                    ConnectionMetadata.NodeType.SERVER,
                    readBuffer,
                    writeBuffer,
                    channel,
                    new ReentrantLock()
            );

            vmsConnectionMetadataMap.put( newVms.hashCode(), connMetadata );

            // setup event receiving for this vms
            channel.read(readBuffer, connMetadata, new VmsReadCompletionHandler() );

        }

    }

    private class LeaderReadCompletionHandler implements CompletionHandler<Integer, ConnectionMetadata> {

        @Override
        public void completed(Integer result, ConnectionMetadata connectionMetadata) {

            byte messageType = connectionMetadata.readBuffer.get();

            // receive input events
            if(messageType == EVENT) {

                TransactionEvent.Payload transactionEventPayload = TransactionEvent.read(connectionMetadata.readBuffer);

                // send to scheduler.... drop if the event cannot be processed (not an input event in this vms)
                if(vmsMetadata.queueToEventMap().get( transactionEventPayload.event() ) != null) {
                    vmsInternalChannels.transactionInputQueue().add(transactionEventPayload);
                }

            } else if (messageType == BATCH_COMMIT_REQUEST){
                // must send batch commit ok
                BatchCommitRequest.Payload batchCommitReq = BatchCommitRequest.read( connectionMetadata.readBuffer );
                vmsInternalChannels.batchCommitQueue().add( batchCommitReq );
            } else if(messageType == TX_ABORT){
                TransactionAbort.Payload transactionAbortReq = TransactionAbort.read( connectionMetadata.readBuffer );
                vmsInternalChannels.transactionAbortInputQueue().add( transactionAbortReq );
            } else if (messageType == BATCH_ABORT_REQUEST){
                // some new leader request to rollback to last batch commit
                BatchAbortRequest.Payload batchAbortReq = BatchAbortRequest.read( connectionMetadata.readBuffer );
                vmsInternalChannels.batchAbortQueue().add(batchAbortReq);
            }

            connectionMetadata.readBuffer.clear();
            connectionMetadata.channel.read(connectionMetadata.readBuffer, connectionMetadata, this);
        }

        @Override
        public void failed(Throwable exc, ConnectionMetadata connectionMetadata) {
            if (connectionMetadata.channel.isOpen()){
                connectionMetadata.channel.read(connectionMetadata.readBuffer, connectionMetadata, this);
            } else {
                leader.off();
            }
        }

    }

}
