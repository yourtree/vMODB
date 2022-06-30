package dk.ku.di.dms.vms.sdk.embed;

import dk.ku.di.dms.vms.sdk.core.event.handler.IVmsEventHandler;
import dk.ku.di.dms.vms.sdk.core.event.channel.IVmsInternalChannels;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadata;
import dk.ku.di.dms.vms.web_common.buffer.BufferManager;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;
import dk.ku.di.dms.vms.web_common.meta.ServerIdentifier;
import dk.ku.di.dms.vms.web_common.meta.VmsIdentifier;
import dk.ku.di.dms.vms.web_common.meta.schema.batch.BatchCommitRequest;
import dk.ku.di.dms.vms.web_common.meta.schema.control.Presentation;
import dk.ku.di.dms.vms.web_common.meta.schema.transaction.TransactionEvent;
import dk.ku.di.dms.vms.web_common.runnable.SignalingStoppableRunnable;
import dk.ku.di.dms.vms.web_common.serdes.IVmsSerdesProxy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

import static dk.ku.di.dms.vms.web_common.meta.Constants.*;
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
    private final ExecutorService executorService;

    /** SERVER SOCKET **/
    // other VMSs may want to connect in order to send events
    private final AsynchronousServerSocketChannel serverSocket;

    /** INTERNAL CHANNELS **/
    private final IVmsInternalChannels vmsInternalChannels;

    /** VMS OWN METADATA **/
    private VmsIdentifier me; // this merges network and semantic data about the vms
    private final VmsMetadata vmsMetadata;

    /** EXTERNAL VMSs **/
    private Map<Integer, ConnectionMetadata> vmsConnectionMetadataMap;

    /** SERIALIZATION **/
    private final IVmsSerdesProxy serdes;

    /** COORDINATOR **/
    private ServerIdentifier leader;
    private ConnectionMetadata leaderConnectionMetadata;

    private volatile long lastTimestamp;

    /** INTERNAL STATE **/
    private long currentTid;
    private long currentBatchOffset;



    public EmbedVmsEventHandler(IVmsInternalChannels vmsInternalChannels,
                                VmsMetadata vmsMetadata,
                                IVmsSerdesProxy serdes,
                                ExecutorService executorService,
                                AsynchronousServerSocketChannel serverSocket,
                                AsynchronousChannelGroup group) {
        super();
        this.vmsInternalChannels = vmsInternalChannels;
        this.vmsMetadata = vmsMetadata;
        this.serdes = serdes;
        this.executorService = executorService;
        this.serverSocket = serverSocket;
    }

    @Override
    public void run() {

        // we must also load internal state
        loadInternalState();

        // setup accept since we need to accept connections from the coordinator and other VMSs
        serverSocket.accept( null, new AcceptCompletionHandler());

        // while not stopped
        while(!isStopped()){

            /*
            // do we have an input payload to process?
            processIncomingEvent();

            // do we have an input data (from sidecar) to process?
            processIncomingData();

            // do we have an output payload to dispatch? can be another thread, the sdk is dominated by IOs
            dispatchOutputEvents();

            // do we have data requests to the sidecar?
            dispatchDataRequests();
            */

        }

    }

    /**
     * Last committed state is read on startup. More to come...
     */
    private void loadInternalState() {
        this.currentTid = 0;
        this.currentBatchOffset = 0;
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
                executorService.submit( () -> processAcceptConnection(channel, buffer));

            } catch(Exception ignored){ } finally {
                // continue listening
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
                leader = Presentation.readServer(readBuffer);

                readBuffer.clear();

                ByteBuffer writeBuffer = BufferManager.loanByteBuffer();

                if(includeMetadata) {
                    String vmsDataSchemaStr = serdes.serializeDataSchema(me.dataSchema);
                    String vmsEventSchemaStr = serdes.serializeEventSchema(me.eventSchema);
                    Presentation.writeVms(writeBuffer, me, vmsDataSchemaStr, vmsEventSchemaStr);
                } else {
                    // simply ACK
                    Presentation.writeAck(writeBuffer, me);
                }

                try {
                    channel.write( writeBuffer ).get();
                    writeBuffer.clear();

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

                } catch (InterruptedException | ExecutionException ignored) {

                    // return buffers
                    BufferManager.returnByteBuffer(readBuffer);
                    BufferManager.returnByteBuffer(writeBuffer);

                    leader.off();

                }

            } else {
                // then it is a vms intending to connect due to a data/event that should be delivered to this vms



            }

        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            if (serverSocket.isOpen()){
                serverSocket.accept(null, this);
            }
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

            } else if( messageType == HEARTBEAT ){
                // same logic as follower, but does not go into leader election, sits idle...
                lastTimestamp = System.nanoTime();
                connectionMetadata.readBuffer.clear();
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

    /*
    private void processIncomingEvent(){

        if(futureEvent.isDone()){

            try {
                futureEvent.get();

                TransactionalEvent event = serdes.deserializeToTransactionalEvent( eventReadBuffer.array() );
                inputQueue.add( event );

            } catch (InterruptedException | ExecutionException e) {
                logger.info("ERROR: "+e.getLocalizedMessage());
            } finally {
                eventReadBuffer.clear();
                // TODO should we do that with completion handler?
                futureEvent = eventChannel.read(eventReadBuffer);
            }

        }

    }

    // Everything should be a DTO
    private void processIncomingData() {

        if(futureData.isDone()){

            try {
                futureData.get();

                // that would be the fulfillment of the future
                DataResponseEvent dataResponse = serdes.deserializeToDataResponseEvent( dataSocketBuffer.array() );

                responseMap.put( dataResponse.identifier, dataResponse );

                DataRequestEvent dataRequestEvent = pendingRequestsMap.remove( dataResponse.identifier );

                // notify the thread that is waiting for this data
                synchronized (dataRequestEvent){
                    dataRequestEvent.notify();
                }

            } catch (InterruptedException | ExecutionException e) {
                logger.log(Level.WARNING, "ERR: on reading the new data: "+ e.getLocalizedMessage());
            } finally {

                dataSocketBuffer.clear();
                futureData = eventChannel.read(eventReadBuffer);
            }

        }

    }

    private void dispatchOutputEvents() {

        while( !vmsInternalChannels.outputQueue().isEmpty() ){

           TransactionalEvent outputEvent = outputQueue.poll();

            byte[] bytes = serdes.serializeTransactionalEvent( outputEvent );
            eventChannel.write(ByteBuffer.wrap(bytes));

        }

    }

    private void dispatchDataRequests() {

        while( !requestQueue.isEmpty() ){

            DataRequestEvent dataRequestEvent = requestQueue.poll();

            pendingRequestsMap.put( dataRequestEvent.identifier, dataRequestEvent );

            byte[] bytes = serdes.serializeDataRequestEvent( dataRequestEvent );

            // TODO build mechanism for send failure
            eventWriteBuffer.clear();
            eventWriteBuffer.put( bytes );

            eventChannel.write(eventWriteBuffer);

        }

    }
    */

    /**
     * This link may help to decide:
     * https://www.ibm.com/docs/en/oala/1.3.5?topic=SSPFMY_1.3.5/com.ibm.scala.doc/config/iwa_cnf_scldc_kfk_prp_exmpl_c.html
     *
     * Look for socket.send.buffer.bytes
     * https://kafka.apache.org/08/documentation.html
     *
     * https://developpaper.com/analysis-of-kafka-network-layer/
     * @param socketChannel
     * @throws IOException
     */
    private void setDefaultSocketOptions( AsynchronousSocketChannel socketChannel ) throws IOException {

        socketChannel.setOption( TCP_NODELAY, Boolean.TRUE ); // false is the default value
        socketChannel.setOption( SO_KEEPALIVE, Boolean.TRUE );

        socketChannel.setOption( SO_SNDBUF, 1024 );
        socketChannel.setOption( SO_RCVBUF, 1024 );

        // TODO revisit these options
        socketChannel.setOption( SO_REUSEADDR, true );
        socketChannel.setOption( SO_REUSEPORT, true );
    }

}
