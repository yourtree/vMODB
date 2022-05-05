package dk.ku.di.dms.vms.sdk.core.client.websocket;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import dk.ku.di.dms.vms.modb.common.event.TransactionalEvent;
import dk.ku.di.dms.vms.sdk.core.event.pubsub.IVmsInternalPubSub;
import dk.ku.di.dms.vms.sdk.core.event.pubsub.VmsInternalPubSub;
import dk.ku.di.dms.vms.sdk.core.example.EventExample;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadataLoader;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsMetadata;
import dk.ku.di.dms.vms.sdk.core.scheduler.VmsTransactionScheduler;
import dk.ku.di.dms.vms.web_common.serdes.TransactionalEventAdapter;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import static java.util.logging.Logger.GLOBAL_LOGGER_NAME;

public class ApplicationTest
{
    private static final Logger logger = Logger.getLogger(GLOBAL_LOGGER_NAME);

    private static Gson gson;

    private static VmsMetadata vmsMetadata;

    @BeforeClass
    public static void setup(){

        try {

            IVmsInternalPubSub internalPubSub = VmsInternalPubSub.newInstance();
            vmsMetadata = VmsMetadataLoader.load("dk.ku.di.dms.vms.sdk.core.example", internalPubSub);

            GsonBuilder builder = new GsonBuilder();
            builder.registerTypeAdapter( TransactionalEvent.class, new TransactionalEventAdapter( vmsMetadata.queueToEventMap() ) );
            builder.setPrettyPrinting();
            gson = builder.create();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testMetadataLoader() {

        // buildCustomWebSocketClient(null);

        // VmsEventHandler handler = new VmsEventHandler( eventChannel.inputQueue(), , serdesProxy );

        // IVmsEventHandler eventHandler = WebSocketHandlerBuilder.build( vmsMetadata ); // new VmsWebSocketClient(serdesProxy, handler);

        //TransactionalEvent transactionalEvent = new TransactionalEvent( 1, "in", new EventExample(1) );

        String json = new Gson().toJson( vmsMetadata.vmsEventSchema() );

        //String json = gson.toJson( transactionalEvent );

        //TransactionalEvent converted = gson.fromJson( json, TransactionalEvent.class );

        assert json != null;
    }

    @Test
    public void testScheduler() throws InterruptedException, ExecutionException {

        IVmsInternalPubSub internalPubSub = VmsInternalPubSub.newInstance();

        VmsTransactionScheduler scheduler = new VmsTransactionScheduler(
                Executors.newFixedThreadPool(1),
                internalPubSub,
                vmsMetadata.eventToVmsTransactionMap()
        );

        Thread t0 = new Thread( scheduler );
        t0.start();

        EventExample event = new EventExample(1);

        TransactionalEvent event1 = new TransactionalEvent( 3, "in", event );
        TransactionalEvent event2 = new TransactionalEvent( 2, "in", event );
        TransactionalEvent event3 = new TransactionalEvent( 1, "in", event );

        CompletableFuture<Void> wait = CompletableFuture.runAsync( () -> internalPubSub.inputQueue().add( event1 ) )
                .thenRun( () -> internalPubSub.inputQueue().add( event2 ) )
                .thenRun( () -> internalPubSub.inputQueue().add( event3 ) );

        wait.get();

        Thread.sleep(1000);

        scheduler.stop();

        assert internalPubSub.outputQueue().size() == 3;

    }

    @Test
    public void testDataLoader() throws Exception {

        IVmsInternalPubSub internalPubSub = VmsInternalPubSub.newInstance();

        VmsMetadata metadata = VmsMetadataLoader.load("dk.ku.di.dms.vms.tpcc", internalPubSub);

//        SyntheticDataLoader dataLoader = metadata.loadedVmsInstances().get(SyntheticDataLoader.class);
//
//        dataLoader.load( 1, 1 );
//
//        NewOrderTransaction newOrderTransaction = metadata.getMicroservice(NewOrderTransaction.class);

        // newOrderTransaction.

    }

    @Test
    public void testDynamicProxying() {
//        IProductRepository proxyInstance = (IProductRepository) Proxy.newProxyInstance(
//                AppTest.class.getClassLoader(),
//                new Class[] { IProductRepository.class },
//                new RepositoryFacade( IProductRepository.class ));
//
//        DummyLogic logic = new DummyLogic(proxyInstance);
//
//        AddProductRequest request = new AddProductRequest();
//
//        logic.addProduct(request);
//
//        Assert.assertTrue(true);
    }


    @Test
    public void testMethodHandles() throws Throwable {

//        Item item = new Item(); item.i_id = 1;
//        Field field = Item.class.getField("i_id");
//        MethodHandle h = MethodHandles.lookup().unreflectGetter(field);
//        Integer value = (Integer) h.invoke( item );
//        IFilter filter = FilterBuilder.getFilter(EQUALS, Integer::compareTo);
//
//        Assert.assertTrue(filter.eval( value ));
    }

    @Test
    public void testCreatingFilter() {

//        IFilter filter = FilterBuilder.getFilter(EQUALS, Integer::compareTo);
//
//        Row row = new Row( 1, 2, 3, 10L );
//        // filter.and
//         Assert.assertTrue(filter.eval( row.get(0) ));

    }

    @Test
    public void testDataOperationExecution() throws Exception {

//        // 3. create an input payload and dispatch for execution
//        EventRepository eventRepository = EventRepository.get();
//        VmsTransactionExecutor executor = new VmsTransactionExecutor(eventRepository);
//
//        // executor.Init();
//
//        // CheckoutRequest payload = new CheckoutRequest();
//        CustomerNewOrderIn payload = new CustomerNewOrderIn(1,1,1);
//
//        eventRepository.inputQueue.add(payload);
//
//        Thread.sleep(100000);
    }

    @Test
    public void testParameterizedCall() {

//        RepositoryFacade facade = new RepositoryFacade( IProductRepository.class );
//
//        Catalog catalog = TestCommon.getDefaultCatalog();
//        Analyzer analyzer = new Analyzer( catalog );
//        facade.setAnalyzer(analyzer);
//        Planner planner = new Planner();
//        facade.setPlanner( planner );
//
//        Table table = catalog.getTable("customer");
//
//        CompositeKey key = new CompositeKey(1L,1,1);
//        Row row = new Row(1L,1,1,1F,"","",1F,1F);
//
//        table.getPrimaryKeyIndex().upsert(key, row);
//
//        IProductRepository proxyInstance = (IProductRepository) Proxy.newProxyInstance(
//                AppTest.class.getClassLoader(),
//                new Class[] { IProductRepository.class },
//                facade);
//
//        DummyLogic logic = new DummyLogic(proxyInstance);
//
//        CheckoutRequest request = new CheckoutRequest();
//
//        logic.checkoutCart(request);
//
//        Assert.assertTrue(true);
    }

}
