package dk.ku.di.dms.vms.sdk.core.event.handler;

import dk.ku.di.dms.vms.modb.common.event.DataRequestEvent;
import dk.ku.di.dms.vms.modb.common.event.DataResponseEvent;
import dk.ku.di.dms.vms.modb.common.event.SystemEvent;
import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;
import dk.ku.di.dms.vms.sdk.core.event.pubsub.IVmsInternalPubSub;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadata;
import dk.ku.di.dms.vms.web_common.meta.VmsEventSchema;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.web_common.serdes.IVmsSerdesProxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.net.StandardSocketOptions.*;
import static java.util.logging.Logger.GLOBAL_LOGGER_NAME;
import static java.util.logging.Logger.getLogger;

/**
 * Rate of consumption of events and data are different, as well as their payload
 */
public class VmsEventHandler extends StoppableRunnable implements IVmsEventHandler {

    private static final Logger logger = getLogger(GLOBAL_LOGGER_NAME);

    /** EXECUTOR SERVICE (for socket channels) **/
    private final ExecutorService executorService;

    /** SOCKET CHANNEL OPERATIONS **/
    private Future<Integer> futureEvent;

    private Future<Integer> futureData;

    private AsynchronousSocketChannel eventChannel;

    private AsynchronousSocketChannel dataChannel;

    // store off-heap to avoid overhead
    private final ByteBuffer eventReadByteBuffer;

    // to avoid allocation of byte buffer on every write
    private final ByteBuffer eventWriteByteBuffer;

    private final ByteBuffer dataSocketByteBuffer;

    /** TRANSACTIONAL EVENTS **/
    private final Queue<TransactionalEvent> outputQueue;

    private final Queue<TransactionalEvent> inputQueue;

    /** DATA **/
    private final Queue<DataRequestEvent> requestQueue;

    private final Map<Long, DataResponseEvent> responseMap;

    private final Map<Long, DataRequestEvent> pendingRequestsMap;

    /** METADATA **/
    private final VmsMetadata vmsMetadata;

    /** SERIALIZATION **/
    private final IVmsSerdesProxy serdes;

    public VmsEventHandler(IVmsInternalPubSub vmsInternalPubSubService, VmsMetadata vmsMetadata, IVmsSerdesProxy serdes, ExecutorService executorService) {

        super();

        this.outputQueue = vmsInternalPubSubService.outputQueue();
        this.inputQueue = vmsInternalPubSubService.inputQueue();

        this.requestQueue = vmsInternalPubSubService.requestQueue();
        this.responseMap = vmsInternalPubSubService.responseMap();

        this.vmsMetadata = vmsMetadata;

        // event
        this.eventReadByteBuffer = ByteBuffer.allocateDirect(1024);
        this.eventWriteByteBuffer = ByteBuffer.allocateDirect(1024);

        // data
        this.dataSocketByteBuffer = ByteBuffer.allocateDirect(1024);

        this.serdes = serdes;

        this.executorService = executorService;

        this.pendingRequestsMap = new HashMap<>();
    }

    @Override
    public void run() {

        initialize();

        // while not stopped
        while(!isStopped()){

            // do we have an input payload to process?
            processNewEvent();

            // do we have an input data (from sidecar) to process?
            processNewData();

            // do we have an output payload to dispatch? can be another thread, the sdk is dominated by IOs
            dispatchOutputEvents();

            // do we have data requests to the sidecar?
            dispatchDataRequests();

        }

    }

    private void processNewEvent(){

        if(futureEvent.isDone()){

            try {
                futureEvent.get();

                TransactionalEvent event = serdes.deserializeToTransactionalEvent( eventReadByteBuffer.array() );
                inputQueue.add( event );

            } catch (InterruptedException | ExecutionException e) {
                logger.info("ERROR: "+e.getLocalizedMessage());
            } finally {
                eventReadByteBuffer.clear();
                // TODO should we do that with completion handler?
                futureEvent = eventChannel.read(eventReadByteBuffer);
            }

        }

    }

    /**
     * Everything should be a DTO
     */
    private void processNewData() {

        if(futureData.isDone()){

            try {
                futureData.get();

                // that would be the fulfillment of the future
                DataResponseEvent dataResponse = serdes.deserializeToDataResponseEvent( dataSocketByteBuffer.array() );

                responseMap.put( dataResponse.identifier, dataResponse );

                DataRequestEvent dataRequestEvent = pendingRequestsMap.remove( dataResponse.identifier );

                // notify the thread that is waiting for this data
                synchronized (dataRequestEvent){
                    dataRequestEvent.notify();
                }

            } catch (InterruptedException | ExecutionException e) {
                logger.log(Level.WARNING, "ERR: on reading the new data: "+ e.getLocalizedMessage());
            } finally {

                dataSocketByteBuffer.clear();
                futureData = eventChannel.read(eventReadByteBuffer);
            }

        }

    }

    private void dispatchOutputEvents() {

        while( !outputQueue.isEmpty() ){

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
            eventWriteByteBuffer.clear();
            eventWriteByteBuffer.put( bytes );

            eventChannel.write( eventWriteByteBuffer );

        }

    }

    /**
     * If no exception is thrown, the initialization worked
     * In other words, the workers can exchange data
     */
    private void initialize() {

        // PHASE 0 -- initialize asynchronous channel group
        AsynchronousChannelGroup eventGroup;
        AsynchronousChannelGroup dataGroup;

        try {
            // FIXME it should be same group....
            eventGroup = AsynchronousChannelGroup.withThreadPool( executorService );
            dataGroup = AsynchronousChannelGroup.withThreadPool( executorService );
        } catch (IOException e) {
            throw new RuntimeException("ERROR on setting up asynchronous channel group.");
        }

        // PHASE 1 - connect both channels
        try {

            // event first
            connectToSidecar("event", eventGroup).get();

            // then data schema
            connectToSidecar("data", dataGroup).get();

        } catch( ExecutionException | InterruptedException e ){
            throw new RuntimeException("ERROR on connecting to sidecar.");
        }


        // PHASE 2- send schemas, this is compared to a handshake in a WebSocket protocol

        // send event schema
        byte[] eventSchemaBytes = serdes.serializeEventSchema( vmsMetadata.vmsEventSchema() );
        eventChannel.write( ByteBuffer.wrap(eventSchemaBytes) );

        // send data schema
        byte[] dataSchemaBytes = serdes.serializeDataSchema( vmsMetadata.vmsDataSchema() );
        dataChannel.write( ByteBuffer.wrap(dataSchemaBytes) );

        // PHASE 3 -  get confirmation whether the schemas are fine

        // to avoid checking for nullability in the payload loop
        // besides, the framework must wait for events either way
        eventReadByteBuffer.clear();
        futureEvent = eventChannel.read(eventReadByteBuffer);

        dataSocketByteBuffer.clear();
        futureData = dataChannel.read(dataSocketByteBuffer);

        try {
            futureEvent.get();
            futureData.get();
        } catch( ExecutionException | InterruptedException e ){
            throw new RuntimeException("ERROR on retrieving handshake from sidecar.");
        }

        SystemEvent eventSchemaResponse = serdes.deserializeSystemEvent( eventReadByteBuffer.array() );

        if(eventSchemaResponse.op == 0){
            throw new RuntimeException(eventSchemaResponse.message);
        }

        SystemEvent dataSchemaResponse = serdes.deserializeSystemEvent( dataSocketByteBuffer.array() );

        if(dataSchemaResponse.op == 0){
            throw new RuntimeException(dataSchemaResponse.message);
        }

        // prepare futures
        eventReadByteBuffer.clear();
        futureEvent = eventChannel.read(eventReadByteBuffer);

        dataSocketByteBuffer.clear();
        futureData = dataChannel.read(dataSocketByteBuffer);

    }

    private void setDefaultSocketOptions( AsynchronousSocketChannel socketChannel ) throws IOException {

        socketChannel.setOption( TCP_NODELAY, Boolean.TRUE ); // false is the default value
        socketChannel.setOption( SO_KEEPALIVE, Boolean.TRUE );

        socketChannel.setOption( SO_SNDBUF, 1024 );
        socketChannel.setOption( SO_RCVBUF, 1024 );

        // TODO revisit these options
        socketChannel.setOption( SO_REUSEADDR, true );
        socketChannel.setOption( SO_REUSEPORT, true );
    }

    /**
     * TODO does this thread pool should be shared among web tasks and app-logic tasks?
     * @param channelName
     * @param group
     * @return Connection future
     */
    private Future<Void> connectToSidecar(String channelName, AsynchronousChannelGroup group){

        try {

            InetSocketAddress address = new InetSocketAddress("localhost", 80);

            if (channelName.equalsIgnoreCase("data")) {

                eventChannel = AsynchronousSocketChannel.open(group);
                setDefaultSocketOptions( eventChannel );
                return eventChannel.connect(address);
            } else{
                dataChannel = AsynchronousSocketChannel.open(group);
                setDefaultSocketOptions( dataChannel );

                // FIXME send vmsmetada as part of the connection

                byte[] eventSchemaBytes = serdes.serializeEventSchema( vmsMetadata.vmsEventSchema() );
//                eventChannel.write( ByteBuffer.wrap(eventSchemaBytes) );
                eventWriteByteBuffer.put( eventSchemaBytes );
                // TODO send only one vms schema, later we figure out how to deal with many... these are the case to explore many-core architectures...


                dataChannel.connect(address, eventWriteByteBuffer, new CompletionHandler<Void, ByteBuffer>() {

                    @Override
                    public void completed(Void result, ByteBuffer attachment) {

                    }

                    @Override
                    public void failed(Throwable exc, ByteBuffer attachment) {

                    }

                });

                return null;

            }

        } catch (IOException e) {
            logger.info("ERROR: "+e.getLocalizedMessage());
            throw new RuntimeException("Error on setting socket options for channel "+channelName);
        }

    }

}
