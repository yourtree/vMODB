package dk.ku.di.dms.vms.sdk.embed;

import dk.ku.di.dms.vms.modb.common.data_structure.KeyValueEntry;
import dk.ku.di.dms.vms.modb.common.data_structure.OneProducerOneConsumerQueue;
import dk.ku.di.dms.vms.modb.common.data_structure.SimpleQueue;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.network.NetworkNode;
import dk.ku.di.dms.vms.modb.common.schema.network.ServerIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.VmsIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchAbortRequest;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.BatchCommitRequest;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.sdk.core.event.channel.IVmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;
import dk.ku.di.dms.vms.web_common.meta.Issue;
import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.*;
import static dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation.*;
import static dk.ku.di.dms.vms.web_common.meta.Issue.Category.CANNOT_CONNECT_TO_NODE;
import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.TCP_NODELAY;

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
public final class EmbedVmsEventHandler extends SignalingStoppableRunnable {

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
    private List<NetworkNode> consumerVms;
    private Map<Integer, ConnectionMetadata> vmsConnectionMetadataMap;

    /** UNKNOWN NODES **/
    private Map<Integer, KeyValueEntry<AsynchronousSocketChannel,ByteBuffer>> unknownNodeChannel;

    /** SERIALIZATION **/
    private final IVmsSerdesProxy serdesProxy;

    /** COORDINATOR **/
    private ServerIdentifier leader;
    private ConnectionMetadata leaderConnectionMetadata;

    /** INTERNAL STATE **/
    private long currentTid;
    private long currentBatchOffset;

    /** Presentation messages to be processed **/
    private SimpleQueue<KeyValueEntry<AsynchronousSocketChannel, ByteBuffer>> presentationMessages;

    public EmbedVmsEventHandler(IVmsInternalChannels vmsInternalChannels, // for communicating with other components
                                VmsIdentifier me, // to identify which vms this is
                                VmsRuntimeMetadata vmsMetadata, // metadata about this vms
                                IVmsSerdesProxy serdesProxy, // ser/des of objects
                                ExecutorService executorService // for recurrent and continuous tasks
    ) throws IOException {
        super();

        this.vmsInternalChannels = vmsInternalChannels;
        this.me = me;
        this.vmsMetadata = vmsMetadata;

        this.serdesProxy = serdesProxy;
        this.taskExecutor = executorService;

        this.group = AsynchronousChannelGroup.withThreadPool(executorService);
        this.serverSocket = AsynchronousServerSocketChannel.open(this.group);
        SocketAddress address = new InetSocketAddress(me.host, me.port);
        serverSocket.bind(address);

        this.unknownNodeChannel = new HashMap<>(10);
        this.presentationMessages =
                new OneProducerOneConsumerQueue<>(10);
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

                if(!vmsInternalChannels.batchCompleteOutputQueue().isEmpty()){
                    // handle

                }

                if(!vmsInternalChannels.transactionAbortOutputQueue().isEmpty()){
                    // handle

                }

                if(!vmsInternalChannels.transactionOutputQueue().isEmpty()){
                    // handle
                    logger.info("New transaction output in event handler!!!");
                }

                if(!presentationMessages.isEmpty()){

                    logger.info("New presentation message for processing!");

                    var presentationMessage = presentationMessages.remove();
                    processPresentationMessage( presentationMessage.getKey(), presentationMessage.getValue() );
                }

            } catch (Exception e) {
                logger.warning("Problem on handling event on event handler.");
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

        String dataSchema = serdesProxy.serializeDataSchema( this.me.dataSchema );
        String eventSchema = serdesProxy.serializeEventSchema( this.me.eventSchema );

        for(NetworkNode vms : this.consumerVms) {

            try {

                InetSocketAddress address = new InetSocketAddress(vms.host, vms.port);
                AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(group);

                channel.setOption(TCP_NODELAY, true);
                channel.setOption(SO_KEEPALIVE, true);

                channel.connect(address).get();

                ConnectionMetadata connMetadata = new ConnectionMetadata(
                        vms.hashCode(),
                        ConnectionMetadata.NodeType.VMS,
                        MemoryManager.getTemporaryDirectBuffer(),
                        MemoryManager.getTemporaryDirectBuffer(), 
                        channel, 
                        new Semaphore(1));

                vmsConnectionMetadataMap.put(vms.hashCode(), connMetadata);

                Presentation.writeVms( connMetadata.writeBuffer, me, dataSchema, eventSchema );

                channel.write( connMetadata.writeBuffer ).get();

                channel.read(connMetadata.readBuffer, connMetadata, new VmsReadCompletionHandler() );

            } catch (IOException | ExecutionException | InterruptedException e) {
                issueQueue.add( new Issue(CANNOT_CONNECT_TO_NODE, vms.hashCode()) );
            }

        }
    }

    private class VmsReadCompletionHandler implements CompletionHandler<Integer, ConnectionMetadata> {

        @Override
        public void completed(Integer result, ConnectionMetadata connectionMetadata) {

            // can only be event, skip reading the message type
            connectionMetadata.readBuffer.position(1);

            // data dependence or input event
            TransactionEvent.Payload transactionEventPayload = TransactionEvent.read(connectionMetadata.readBuffer);

            // send to scheduler
            if(vmsMetadata.queueToEventMap().get( transactionEventPayload.event() ) != null) {
                vmsInternalChannels.transactionInputQueue().add(transactionEventPayload);
            }

            connectionMetadata.readBuffer.clear();
            connectionMetadata.channel.read(connectionMetadata.readBuffer, connectionMetadata, this);

        }

        @Override
        public void failed(Throwable exc, ConnectionMetadata connectionMetadata) {
            if (connectionMetadata.channel.isOpen()){
                connectionMetadata.channel.read(connectionMetadata.readBuffer, connectionMetadata, this);
            }
        }
    }

    /**
     * On an connection attempt, it is unknow what is the type of node
     * attempting the connection. We find out after the first read.
     */
    private class UnknownNodeReadCompletionHandler implements CompletionHandler<Integer, Integer> {

        @Override
        public void completed(Integer result, Integer channelIdentifier) {

            // send to main thread. the main thread handles writes and internal tasks
            KeyValueEntry<AsynchronousSocketChannel,ByteBuffer> kvEntry = unknownNodeChannel.remove(channelIdentifier);

            if(kvEntry != null) presentationMessages.add( kvEntry );

            try {
                //logger.info("A read has been completed for "+kvEntry.getKey().getRemoteAddress());
                logger.info("Presentation message for "+ kvEntry.getKey().getRemoteAddress() +" has been queued for processing");
            } catch (IOException ignored) { }

        }

        @Override
        public void failed(Throwable exc, Integer channelIdentifier) {
            // release the buffer, nothing else to do
            KeyValueEntry<AsynchronousSocketChannel,ByteBuffer> kvEntry = unknownNodeChannel.remove(channelIdentifier);
            if(kvEntry != null) MemoryManager.releaseTemporaryDirectBuffer(kvEntry.getValue());
        }

    }

    /**
     * Class is iteratively called by the socket pool threads.
     */
    private class AcceptCompletionHandler implements CompletionHandler<AsynchronousSocketChannel, Void> {

        @Override
        public void completed(AsynchronousSocketChannel channel, Void void_) {



            final ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer();

            try {

                logger.info("An unknown host has started a connection attempt. Remote address: "+channel.getRemoteAddress());

                channel.setOption(TCP_NODELAY, true);
                channel.setOption(SO_KEEPALIVE, true);

                KeyValueEntry<AsynchronousSocketChannel,ByteBuffer> kvEntry = new KeyValueEntry<>(channel, buffer);
                unknownNodeChannel.put( channel.getRemoteAddress().hashCode(), kvEntry );

                // read presentation message. if vms, receive metadata, if follower, nothing necessary
                channel.read( buffer, channel.getRemoteAddress().hashCode(), new UnknownNodeReadCompletionHandler() );

                logger.info("Read handler for unknown host has been setup: "+channel.getRemoteAddress());

            } catch(Exception ignored){

            } finally {
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

    private void processPresentationMessage(AsynchronousSocketChannel channel, ByteBuffer readBuffer) {

        logger.info("Starting process for processing presentation message.");

        // message identifier
        byte messageIdentifier = readBuffer.get(1);
        readBuffer.position(2);

        if(messageIdentifier == SERVER_TYPE){

            boolean includeMetadata = readBuffer.get() == YES;

            // leader has disconnected, or new leader
            Presentation.PayloadFromServer payloadFromServer = Presentation.readServer(readBuffer, serdesProxy);

            // only connects to all VMSs on first leader connection
            boolean connectToVMSs = false;
            if(leader == null || !leader.isActive()) {

                this.leader = payloadFromServer.serverIdentifier();
                this.leader.on();

                connectToVMSs = true;
                this.consumerVms = payloadFromServer.consumers();
                
                ByteBuffer writeBuffer = MemoryManager.getTemporaryDirectBuffer();

                readBuffer.clear();

                if(includeMetadata) {
                    writeBuffer.clear();
                    String vmsDataSchemaStr = serdesProxy.serializeDataSchema(me.dataSchema);
                    String vmsEventSchemaStr = serdesProxy.serializeEventSchema(me.eventSchema);

                    Presentation.writeVms(writeBuffer, me, vmsDataSchemaStr, vmsEventSchemaStr);
                    writeBuffer.clear();

                    try {
                        // the protocol requires the leader to wait for the metadata in order to start sending messages
                        channel.write( writeBuffer ).get();
                        writeBuffer.clear();
                    } catch (InterruptedException | ExecutionException ignored) {

                        if(!channel.isOpen()) {
                            leader.off();

                            // return buffers
                            MemoryManager.releaseTemporaryDirectBuffer(readBuffer);
                            MemoryManager.releaseTemporaryDirectBuffer(writeBuffer);

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
                        new Semaphore(1)
                );

            } else {
                this.leader = payloadFromServer.serverIdentifier(); // this will set on again
                leaderConnectionMetadata.channel = channel; // update channel
                this.leader.on();
            }

            channel.read(readBuffer, leaderConnectionMetadata, new LeaderReadCompletionHandler() );

            if(connectToVMSs && !payloadFromServer.consumers().isEmpty()) connectToConsumerVMSs();

        } else if(messageIdentifier == VMS_TYPE) {
            // then it is a vms intending to connect due to a data/event
            // that should be delivered to this vms

            VmsIdentifier newVms = Presentation.readVms(readBuffer, serdesProxy);

            readBuffer.clear();

            ByteBuffer writeBuffer = MemoryManager.getTemporaryDirectBuffer();

            ConnectionMetadata connMetadata = new ConnectionMetadata(
                    leader.hashCode(),
                    ConnectionMetadata.NodeType.SERVER,
                    readBuffer,
                    writeBuffer,
                    channel,
                    new Semaphore(1)
            );

            vmsConnectionMetadataMap.put( newVms.hashCode(), connMetadata );

            // setup event receiving for this vms
            channel.read(readBuffer, connMetadata, new VmsReadCompletionHandler() );

        } else {
            logger.warning("Presentation message from unknown source:"+messageIdentifier);
        }

    }

    private class LeaderReadCompletionHandler implements CompletionHandler<Integer, ConnectionMetadata> {

        @Override
        public void completed(Integer result, ConnectionMetadata connectionMetadata) {

            logger.info("Leader has entered in contact. Let's check what the leader wants.");

            connectionMetadata.readBuffer.position(0);
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
