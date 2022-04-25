package dk.ku.di.dms.vms.sdk.core.event.handler;

import dk.ku.di.dms.vms.modb.common.event.DataRequestEvent;
import dk.ku.di.dms.vms.modb.common.event.DataResponseEvent;
import dk.ku.di.dms.vms.modb.common.event.SystemEvent;
import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;
import dk.ku.di.dms.vms.sdk.core.event.pubsub.IVmsInternalPubSubService;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadata;
import dk.ku.di.dms.vms.web_common.serdes.IVmsSerdesProxy;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.net.StandardSocketOptions.*;
import static java.util.logging.Logger.GLOBAL_LOGGER_NAME;
import static java.util.logging.Logger.getLogger;

/**
 * Rate of consumption of events and data are different, as well as their payload
 */
@ThreadSafe
public class VmsEventHandler implements IVmsEventHandler, Runnable {

    private static final Logger logger = getLogger(GLOBAL_LOGGER_NAME);

    private AtomicBoolean state;

    private AsynchronousSocketChannel eventChannel;

    private AsynchronousSocketChannel dataChannel;

    // store off-heap to avoid overhead
    private final ByteBuffer eventSocketByteBuffer;

    private final ByteBuffer dataSocketByteBuffer;

    private Future<Integer> futureEvent;

    private Future<Integer> futureData;

    /** TRANSACTIONAL EVENTS **/
    private final Queue<TransactionalEvent> outputQueue;

    private final Queue<TransactionalEvent> inputQueue;

    /** DATA **/
    private final Queue<DataRequestEvent> requestQueue;

    private final Map<Long, DataResponseEvent> responseMap;

    private final Map<Long, DataRequestEvent> pendingRequestsMap;

    private final VmsMetadata vmsMetadata;

    // for complex objects like schema definitions
    private final IVmsSerdesProxy serdes;

    public VmsEventHandler( IVmsInternalPubSubService vmsInternalPubSubService, VmsMetadata vmsMetadata, IVmsSerdesProxy serdes ) {

        this.outputQueue = vmsInternalPubSubService.outputQueue();
        this.inputQueue = vmsInternalPubSubService.inputQueue();

        this.requestQueue = vmsInternalPubSubService.requestQueue();
        this.responseMap = vmsInternalPubSubService.responseMap();

        this.vmsMetadata = vmsMetadata;

        this.eventSocketByteBuffer = ByteBuffer.allocateDirect(1024);
        this.dataSocketByteBuffer = ByteBuffer.allocateDirect(1024);

        this.serdes = serdes;

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
            // TODO define an interface for data requests
            dispatchDataRequests();

        }

    }

    private void processNewEvent(){

        if(futureEvent.isDone()){

            try {
                futureEvent.get();

                TransactionalEvent event = serdes.deserializeToTransactionalEvent( eventSocketByteBuffer.array() );
                inputQueue.add( event );

            } catch (InterruptedException | ExecutionException e) {
                logger.info("ERROR: "+e.getLocalizedMessage());
            } finally {
                eventSocketByteBuffer.clear();
                futureEvent = eventChannel.read(eventSocketByteBuffer);
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

                DataResponseEvent dataResponse = serdes.deserializeToDataResponseEvent( dataSocketByteBuffer.array() );

                responseMap.put( dataResponse.identifier, dataResponse );

                DataRequestEvent dataRequestEvent = pendingRequestsMap.remove( dataResponse.identifier );

                // notify the thread that is waiting for this data
                synchronized (dataRequestEvent){
                    dataRequestEvent.notify();
                }


            } catch (InterruptedException | ExecutionException e) {
                logger.log(Level.WARNING, "ERR: on reading the new data");
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
            eventChannel.write(ByteBuffer.wrap( bytes ) );

        }

    }

    private void initialize() {

        //connect both channels
        state = new AtomicBoolean( connectToSidecar("event") && connectToSidecar("data") );

        // send schemas, this is compared to a handshake in a WebSocket protocol

        // send event schema
        byte[] eventSchemaBytes = serdes.serializeEventSchema( vmsMetadata.vmsEventSchema() );
        eventChannel.write( ByteBuffer.wrap(eventSchemaBytes) );

        // send data schema
        byte[] dataSchemaBytes = serdes.serializeDataSchema( vmsMetadata.vmsDataSchema() );
        dataChannel.write( ByteBuffer.wrap(dataSchemaBytes) );

        // now get confirmation whether the schema is fine

        // to avoid checking for nullability in the payload loop
        // besides, the framework must wait for events either way
        eventSocketByteBuffer.clear();
        futureEvent = eventChannel.read(eventSocketByteBuffer);

        dataSocketByteBuffer.clear();
        futureData = dataChannel.read(dataSocketByteBuffer);

        try {
            futureEvent.get();
            futureData.get();
        } catch( ExecutionException | InterruptedException e ){
            throw new RuntimeException("ERROR on retrieving handshake from sidecar.");
        }

        SystemEvent eventSchemaResponse = serdes.deserializeSystemEvent( eventSocketByteBuffer.array() );

        if(eventSchemaResponse.op == 0){
            throw new RuntimeException(eventSchemaResponse.message);
        }

        SystemEvent dataSchemaResponse = serdes.deserializeSystemEvent( dataSocketByteBuffer.array() );

        if( dataSchemaResponse.op == 0 ){
            throw new RuntimeException(dataSchemaResponse.message);
        }

    }

    private void setDefaultSocketOptions( AsynchronousSocketChannel socketChannel ) throws IOException {

        socketChannel.setOption( TCP_NODELAY, Boolean.FALSE ); // false is the default value
        socketChannel.setOption( SO_KEEPALIVE, Boolean.TRUE );

        socketChannel.setOption( SO_SNDBUF, 1024 );
        socketChannel.setOption( SO_RCVBUF, 1024 );

        // TODO revisit these options
        socketChannel.setOption( SO_REUSEADDR, true );
        socketChannel.setOption( SO_REUSEPORT, true );
    }

    private boolean connectToSidecar(String channelName){

        try {

            InetSocketAddress address = new InetSocketAddress("localhost", 80);

            if (channelName.equalsIgnoreCase("data")) {
                eventChannel = AsynchronousSocketChannel.open();
                setDefaultSocketOptions( eventChannel );
                eventChannel.connect(address).get();
            } else{
                dataChannel = AsynchronousSocketChannel.open();
                setDefaultSocketOptions( dataChannel );
                dataChannel.connect( address ).get();
            }
            return true;

        } catch (ExecutionException | InterruptedException | IOException e) {
            logger.info("ERROR: "+e.getLocalizedMessage());
        }
        return false;
    }

    @Override
    public void accept(TransactionalEvent event) {
        this.outputQueue.add( event );
    }

    public boolean isStopped() {
        return !state.get();
    }

    @Override
    public void stop() {
        state.set(false);
    }

}
