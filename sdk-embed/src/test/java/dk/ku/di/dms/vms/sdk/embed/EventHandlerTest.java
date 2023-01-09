package dk.ku.di.dms.vms.sdk.embed;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.VmsEventSchema;
import dk.ku.di.dms.vms.modb.common.schema.network.NetworkNode;
import dk.ku.di.dms.vms.modb.common.schema.network.VmsIdentifier;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.transaction.TransactionFacade;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionResult;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.events.InputEventExample1;
import dk.ku.di.dms.vms.sdk.embed.events.OutputEventExample1;
import dk.ku.di.dms.vms.sdk.embed.events.OutputEventExample2;
import dk.ku.di.dms.vms.sdk.embed.events.OutputEventExample3;
import dk.ku.di.dms.vms.sdk.embed.handler.EmbedVmsEventHandler;
import dk.ku.di.dms.vms.sdk.embed.metadata.EmbedMetadataLoader;
import dk.ku.di.dms.vms.sdk.embed.scheduler.EmbedVmsTransactionScheduler;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test vms event handler
 * a. Are events being ingested correctly?
 * b. are events being outputted correctly?
 * c. is the internal state being managed properly (respecting correctness semantics)?
 * d. all tables have data structures properly created? embed metadata loader
 * e. ingestion is being performed correctly?
 * f. repository facade is working properly?
 *  -
 *  For tests involving the leader, like batch, may need to have a fake leader thread
 *  For this class, transactions and isolation are not involved. We use simple dumb tasks
 *  that do not interact with database state
 */
public class EventHandlerTest {

    private final IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

    /**
     * Facility to load virtual microservice instances.
     * For tests, it may the case two VMSs are in the same package. That leads to
     * buggy metadata being loaded (i.e., swapped in-and-out events)
     * To avoid that, this method takes into consideration the inputs and output events
     * to be discarded for a given
     */
    private static EmbedVmsEventHandler loadMicroserviceDefault(NetworkNode node, Map<String, NetworkNode> consumerVms, String vmsName, String packageName) throws IOException {
        var emptyList = Collections.<String>emptyList();
        return loadMicroservice(node,consumerVms, true, new VmsEmbedInternalChannels(),vmsName,packageName,emptyList,emptyList,emptyList,emptyList);
    }
    private static EmbedVmsEventHandler loadMicroservice(NetworkNode node, Map<String, NetworkNode> consumerVms, boolean eventHandlerActive,
                                                         VmsEmbedInternalChannels vmsInternalPubSubService, String vmsName, String packageName,
                                                         List<String> inToDiscard, List<String> outToDiscard, List<String> inToSwap, List<String> outToSwap) throws IOException  {

        VmsRuntimeMetadata vmsMetadata = EmbedMetadataLoader.loadRuntimeMetadata(packageName);

        // discard events
        for(String in : inToDiscard)
            vmsMetadata.inputEventSchema().remove(in);

        for(String out : outToDiscard)
            vmsMetadata.outputEventSchema().remove(out);

        for(String in : inToSwap) {
            VmsEventSchema eventSchema = vmsMetadata.inputEventSchema().remove(in);
            vmsMetadata.outputEventSchema().put(in, eventSchema);
        }

        for(String in : outToSwap) {
            VmsEventSchema eventSchema = vmsMetadata.outputEventSchema().remove(in);
            vmsMetadata.inputEventSchema().put(in, eventSchema);
        }

        TransactionFacade transactionFacade = EmbedMetadataLoader.loadTransactionFacade(vmsMetadata);

        assert vmsMetadata != null;

        ExecutorService readTaskPool = Executors.newSingleThreadExecutor();

        IVmsSerdesProxy serdes = VmsSerdesProxyBuilder.build();

        EmbedVmsTransactionScheduler scheduler =
                new EmbedVmsTransactionScheduler(
                        readTaskPool,
                        vmsInternalPubSubService,
                        vmsMetadata.queueToVmsTransactionMap(),
                        vmsMetadata.queueToEventMap(),
                        serdes,
                        transactionFacade);

        VmsIdentifier vmsIdentifier = new VmsIdentifier(
                node.host, node.port, vmsName,
                0, 0,
                vmsMetadata.dataSchema(),
                vmsMetadata.inputEventSchema(), vmsMetadata.outputEventSchema());

        ExecutorService socketPool = Executors.newFixedThreadPool(2);

        EmbedVmsEventHandler eventHandler = EmbedVmsEventHandler.build(
                vmsInternalPubSubService, vmsIdentifier, consumerVms, vmsMetadata, serdes, socketPool );

        if(eventHandlerActive) {
            Thread eventHandlerThread = new Thread(eventHandler);
            eventHandlerThread.start();
        }

        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.start();

        return eventHandler;

    }

    /**
     * In this test, the events are serialized and sent directly to
     * the internal channels, and not through the network
     * The idea is to isolate the bug if that appears.
     * I.e., is there a problem with the embed scheduler?
     */
    @Test
    public void testCrossVmsTransactionWithoutEventHandler() throws IOException, InterruptedException {

        List<String> inToDiscard = Collections.emptyList();
        List<String> outToDiscard = List.of("out3");
        List<String> inToSwap = List.of("out2");

        // to avoid set up this thread as a producer
        VmsEmbedInternalChannels channelForAddingInput = new VmsEmbedInternalChannels();

        // microservice 1
        loadMicroservice(
                new NetworkNode("localhost", 1080),
                null,
                false,
                channelForAddingInput,
                "example1",
                "dk.ku.di.dms.vms.sdk.embed",
                inToDiscard,
                outToDiscard,
                inToSwap,
                inToDiscard);

        List<String> outToSwap = List.of("out1");;
        inToSwap = inToDiscard; // empty
        outToDiscard = inToDiscard;
        inToDiscard = List.of("in");

        // internal channel so this thread can read the output at some point without being a consumer
        VmsEmbedInternalChannels channelForGettingOutput = new VmsEmbedInternalChannels();

        // microservice 2
        loadMicroservice(
                new NetworkNode("localhost", 1081),
                null,
                false,
                channelForGettingOutput,
                "example2",
                "dk.ku.di.dms.vms.sdk.embed",
                inToDiscard,
                outToDiscard,
                inToSwap,
                outToSwap);

        InputEventExample1 eventExample = new InputEventExample1(0);
        String payload = this.serdes.serialize(eventExample, InputEventExample1.class);
        TransactionEvent.Payload event = TransactionEvent.of(1,0,1,"in", payload);
        channelForAddingInput.transactionInputQueue().add(event);

        var input1ForVms2 = channelForAddingInput.transactionOutputQueue().take();

        for(var res : input1ForVms2.resultTasks){
            Class<?> clazz =  res.outputQueue().equalsIgnoreCase("out1") ? OutputEventExample1.class : OutputEventExample2.class;
            String serialized = serdes.serialize(res.output(), clazz);
            TransactionEvent.Payload payload_ = TransactionEvent.of(1,0,1,res.outputQueue(), serialized);
            channelForGettingOutput.transactionInputQueue().add(payload_);
        }

        // subscribe to output event out3
        VmsTransactionResult result = channelForGettingOutput.transactionOutputQueue().poll(1000, TimeUnit.SECONDS);

        assert result != null && result.resultTasks != null && !result.resultTasks.isEmpty();

        assert ((OutputEventExample3) result.resultTasks.get(0).output()).id == 2;
    }

    /**
     * Producers are VMSs that intend to send data
     */
    @Test
    public void testConnectionToProducer(){

        // 1 - assemble a producer network node
        // 2 - start a socket in the prescribed node
        // 3 - start the vms 1. don't need to pass info to vms 1, since vms is always waiting for producer
        // 4 - connect to the vms 1
        // 5 - send event input
        // 6 - listen from the internal channel

        assert true;
    }

    /**
     * Consumers are VMSs that intend to receive data from a given VMS
     * Originally the coordinator sends the set of consumer VMSs to each VMS
     * This is because the coordinator holds the transaction workflow definition
     * and from that it derives the consumers for each VMS
     * But for the purpose of this test, we open the interface of the event handler,
     * so we can pass the consumer without having to wait for a coordinator message
     */
    @Test
    public void testConnectionToConsumer() throws IOException, InterruptedException, ExecutionException {

        // 1 - assemble a consumer network node
        NetworkNode me = new NetworkNode("localhost", 1080);

        // 2 - start a socket in the prescribed node
        AsynchronousServerSocketChannel serverSocket = AsynchronousServerSocketChannel.open();
        SocketAddress address = new InetSocketAddress(me.host, me.port);
        serverSocket.bind(address);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean(false);
        final AsynchronousSocketChannel[] channel = {null};

        // 3 - setup accept connection
        serverSocket.accept(null, new CompletionHandler<>() {
            @Override
            public void completed(AsynchronousSocketChannel result, Object attachment) {
                channel[0] = result;
                success.getAndSet(true);
                latch.countDown();
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                latch.countDown();
            }
        });

        Map<String, NetworkNode> consumerSet = new HashMap<>();
        consumerSet.put("out1", me);

        List<String> inToDiscard = Collections.emptyList();
        List<String> outToDiscard = List.of("out3");
        List<String> inToSwap = List.of("out2");

        // 3 - start the vms 1. make sure the info that this is a vms consumer is passed as parameter
        EmbedVmsEventHandler embedVmsEventHandler = loadMicroservice(
                new NetworkNode("localhost", 1081),
                consumerSet,
                true,
                new VmsEmbedInternalChannels(),
                "example1",
                "dk.ku.di.dms.vms.sdk.embed",
                inToDiscard,
                outToDiscard,
                inToSwap,
                inToDiscard);

        // 3 - wait for vms 1 to connect
        latch.await();

        assert success.get();

        // 4 - read
        ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer();
        channel[0].read(buffer).get();
        buffer.position(2);

        VmsIdentifier producerVms = Presentation.readVms(buffer, VmsSerdesProxyBuilder.build());

        embedVmsEventHandler.stop();
        serverSocket.close();

        // 5 - check whether the presentation sent is correct
        assert producerVms.inputEventSchema.containsKey("in") &&
                producerVms.outputEventSchema.containsKey("out1") &&
                producerVms.outputEventSchema.containsKey("out2");

    }

    @Test
    public void testCrossVmsTransactionWithEventHandler() throws IOException, InterruptedException {
        // TODO setup a consumer to subscribe to both out1 and out2
        //  set up producer too, check if events are received end-to-end through the network
    }

    @Test
    public void testBatchCompletion(){
        assert true;
    }

    @Test
    public void testConnectionFromLeader(){
        assert true;
    }

    @Test
    public void testReceiveEventFromLeader(){
        assert true;
    }

}
