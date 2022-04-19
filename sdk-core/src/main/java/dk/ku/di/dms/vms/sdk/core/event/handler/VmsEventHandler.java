package dk.ku.di.dms.vms.sdk.core.event.handler;

import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;
import dk.ku.di.dms.vms.web_common.serdes.IVmsSerdesProxy;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.FileChannel;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.TCP_NODELAY;
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

    private static final int EVENT_SIZE = 1024;

    private static final int DATA_SIZE = 1024;

    // store off-heap to avoid overhead
    private final ByteBuffer eventSocketByteBuffer;

    private final ByteBuffer dataSocketByteBuffer;

    private Future<Integer> futureEvent;

    private Future<Integer> futureData;

    private final Queue<TransactionalEvent> outputQueue;

    // this should be the scheduler
    private final Consumer<TransactionalEvent> consumer;

    private final IVmsSerdesProxy serdes;

    private final static int MEGABYTE  = 1024 * 1024;
    // 1 mega = 1024 * 1024
    private final static int DEFAULT_EVENT_STORE_SIZE = MEGABYTE;

    private final static int DEFAULT_DATA_STORE_SIZE = MEGABYTE;

    private RandomAccessFile rafEvent = null;

    private RandomAccessFile rafData = null;

    private ByteBuffer eventStoreByteBuffer;

    public VmsEventHandler(Queue<TransactionalEvent> outputQueue, Consumer<TransactionalEvent> consumer, IVmsSerdesProxy serdes) {
        this.outputQueue = outputQueue;
        this.consumer = consumer;
        this.eventSocketByteBuffer = ByteBuffer.allocateDirect(EVENT_SIZE);
        this.dataSocketByteBuffer = ByteBuffer.allocateDirect(EVENT_SIZE);
        this.serdes = serdes;
    }

    @Override
    public void run() {

        initialize();

        // while not stopped
        while(!isStopped()){

            // do I have an input event to process?
            receiveNewEvent();

            // do I have an input data (from sidecar) to process?
//            processRequestedData();

            // do I have an output event to dispatch?
            dispatchOutputEvents();

            // do I have data requests to the sidecar?


        }

    }

    private void dispatchOutputEvents() {



    }

    private void initialize(){

        // initialize memory-mapped file
        try {

            File file = new File("/etc/vms-events");
            rafEvent = new RandomAccessFile(file, "rw");
            rafEvent.setLength( DEFAULT_EVENT_STORE_SIZE );
            FileChannel eventChannel = rafEvent.getChannel();
            eventStoreByteBuffer = eventChannel.map( FileChannel.MapMode.READ_WRITE, 0, DEFAULT_EVENT_STORE_SIZE );


        } catch (IOException e) {
            e.printStackTrace();
        }


        state = new AtomicBoolean( connectToSidecar("event") && connectToSidecar("data") );

        // to avoid checking for nullability in the event loop
        eventSocketByteBuffer.clear();
        futureEvent = eventChannel.read(eventSocketByteBuffer);
        dataSocketByteBuffer.clear();
        futureData = dataChannel.read(dataSocketByteBuffer);

    }

    private void receiveNewEvent(){

        if(futureEvent.isDone()){

            // TODO read and dispatch
            try {
                futureEvent.get();
                // TODO should this variable be off-heap?
                String json = new String( eventSocketByteBuffer.array(), 0, EVENT_SIZE );
                TransactionalEvent event = serdes.fromJson( json );
                consumer.accept( event );
            } catch (InterruptedException | ExecutionException e) {
                logger.info("ERROR: "+e.getLocalizedMessage());
            } finally {
                eventSocketByteBuffer.clear();
                futureEvent = eventChannel.read(eventSocketByteBuffer);
            }

        }

    }


    private boolean connectToSidecar(String channelName){

        try {

            InetSocketAddress addr = new InetSocketAddress("localhost", 80);

            if (channelName.equalsIgnoreCase("data")) {
                eventChannel = AsynchronousSocketChannel.open();
                eventChannel.setOption( TCP_NODELAY, Boolean.FALSE );
                eventChannel.setOption( SO_KEEPALIVE, Boolean.TRUE );
                eventChannel.connect(addr).get();
            } else{
                dataChannel = AsynchronousSocketChannel.open();
                dataChannel.setOption( TCP_NODELAY, Boolean.FALSE );
                dataChannel.setOption( SO_KEEPALIVE, Boolean.TRUE );
                dataChannel.connect( addr ).get();
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
