package dk.ku.di.dms.vms.sdk.embed;

import dk.ku.di.dms.vms.sdk.core.event.handler.IVmsEventHandler;
import dk.ku.di.dms.vms.sdk.core.event.channel.IVmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadata;
import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;
import dk.ku.di.dms.vms.web_common.buffer.BufferManager;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;
import dk.ku.di.dms.vms.web_common.meta.Issue;
import dk.ku.di.dms.vms.web_common.meta.ServerIdentifier;
import dk.ku.di.dms.vms.web_common.meta.VmsIdentifier;
import dk.ku.di.dms.vms.web_common.meta.schema.batch.BatchCommitRequest;
import dk.ku.di.dms.vms.web_common.meta.schema.control.Presentation;
import dk.ku.di.dms.vms.web_common.meta.schema.transaction.TransactionEvent;
import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;
import dk.ku.di.dms.vms.web_common.serdes.IVmsSerdesProxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

import static dk.ku.di.dms.vms.web_common.meta.Constants.*;
import static dk.ku.di.dms.vms.web_common.meta.Issue.Category.CANNOT_CONNECT_TO_NODE;
import static dk.ku.di.dms.vms.web_common.meta.schema.control.Presentation.SERVER_TYPE;
import static dk.ku.di.dms.vms.web_common.meta.schema.control.Presentation.YES;
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
    private final VmsMetadata vmsMetadata;

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
                                VmsMetadata vmsMetadata, // metadata about this vms
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
        while(!isStopped()){

            //  write events to leader and VMSs...

            try {

                OutboundEventResult outboundEventRes = vmsInternalChannels.transactionOutputQueue().take();



            } catch (InterruptedException e) {
                e.printStackTrace();
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
                withDefaultSocketOptions( channel );

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
                issueQueue.add( new Issue(CANNOT_CONNECT_TO_NODE, vms) );
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
            if(leader == null) {
                connectToVMSs = true;
                this.consumerVms = payloadFromServer.consumers();
            }
            this.leader = payloadFromServer.serverIdentifier();

            readBuffer.clear();

            ByteBuffer writeBuffer = BufferManager.loanByteBuffer();

            if(includeMetadata) {
                String vmsDataSchemaStr = serdesProxy.serializeDataSchema(me.dataSchema);
                String vmsEventSchemaStr = serdesProxy.serializeEventSchema(me.eventSchema);
                Presentation.writeVms(writeBuffer, me, vmsDataSchemaStr, vmsEventSchemaStr);

                try {
                    channel.write( writeBuffer ).get();
                    writeBuffer.clear();
                } catch (InterruptedException | ExecutionException ignored) {

                    // return buffers
                    BufferManager.returnByteBuffer(readBuffer);
                    BufferManager.returnByteBuffer(writeBuffer);

                    if(!channel.isOpen())
                        leader.off();
                    // else what to do try again?

                    return;

                }

            }

            leader.on();

            // then setup connection metadata and read completion handler
            leaderConnectionMetadata = new ConnectionMetadata(
                    leader.hashCode(),
                    ConnectionMetadata.NodeType.SERVER,
                    readBuffer,
                    writeBuffer,
                    channel,
                    new ReentrantLock()
            );

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

                connectionMetadata.readBuffer.clear();

                // send to scheduler.... drop if the event cannot be processed (not an input event in this vms)
                if(vmsMetadata.queueToEventMap().get( transactionEventPayload.event() ) != null) {
                    vmsInternalChannels.transactionInputQueue().add(transactionEventPayload);
                }

            } else if (messageType == BATCH_COMMIT_REQUEST){

                // must send batch commit ok
                BatchCommitRequest.read( connectionMetadata.readBuffer );

                connectionMetadata.readBuffer.clear();

                //connectionMetadata.writeLock.lock();

                // actually need to log state (but only if all events have been processed)

                //connectionMetadata.writeLock.unlock();

            } else if(messageType == TX_ABORT){

                //

            }

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
