package dk.ku.di.dms.vms;

import static dk.ku.di.dms.vms.database.query.parser.enums.ExpressionTypeEnum.EQUALS;
import static org.junit.Assert.assertTrue;

import dk.ku.di.dms.vms.database.api.modb.RepositoryFacade;
import dk.ku.di.dms.vms.database.catalog.Catalog;
import dk.ku.di.dms.vms.database.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.database.query.planner.Planner;
import dk.ku.di.dms.vms.database.query.planner.operator.filter.FilterBuilder;
import dk.ku.di.dms.vms.database.query.planner.operator.filter.IFilter;
import dk.ku.di.dms.vms.database.store.common.CompositeKey;
import dk.ku.di.dms.vms.database.store.row.Row;
import dk.ku.di.dms.vms.database.store.table.Table;
import dk.ku.di.dms.vms.eShopOnContainers.events.CheckoutRequest;
import dk.ku.di.dms.vms.metadata.VmsMetadata;
import dk.ku.di.dms.vms.metadata.VmsMetadataLoader;
import dk.ku.di.dms.vms.modb.TestCommon;
import dk.ku.di.dms.vms.operational.DataOperationExecutor;
import dk.ku.di.dms.vms.event.EventRepository;
import dk.ku.di.dms.vms.eShopOnContainers.events.AddProductRequest;
import dk.ku.di.dms.vms.eShopOnContainers.logic.DummyLogic;
import dk.ku.di.dms.vms.eShopOnContainers.repository.IProductRepository;
import dk.ku.di.dms.vms.tpcc.entity.Item;
import dk.ku.di.dms.vms.tpcc.events.CustomerNewOrderIn;
import dk.ku.di.dms.vms.tpcc.workload.NewOrderTransaction;
import dk.ku.di.dms.vms.tpcc.workload.SyntheticDataLoader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;

public class AppTest 
{
    private static Logger log = LoggerFactory.getLogger(AppTest.class);

    @Test
    public void testMetadataLoader() throws Exception {

        VmsMetadataLoader loader = new VmsMetadataLoader();
        VmsMetadata config = loader.load("dk.ku.di.dms.vms.tpcc");

    }

    @Test
    public void testDataLoader() throws Exception {

        VmsMetadataLoader loader = new VmsMetadataLoader();
        VmsMetadata metadata = loader.load("dk.ku.di.dms.vms.tpcc");

        SyntheticDataLoader dataLoader = metadata.getMicroservice(SyntheticDataLoader.class);

        dataLoader.load( 1, 1 );

        NewOrderTransaction newOrderTransaction = metadata.getMicroservice(NewOrderTransaction.class);

        // newOrderTransaction.

    }

    @Test
    public void testDynamicProxying() {
        IProductRepository proxyInstance = (IProductRepository) Proxy.newProxyInstance(
                AppTest.class.getClassLoader(),
                new Class[] { IProductRepository.class },
                new RepositoryFacade( IProductRepository.class ));

        DummyLogic logic = new DummyLogic(proxyInstance);

        AddProductRequest request = new AddProductRequest();

        logic.addProduct(request);

        assertTrue(true);
    }


    @Test
    public void testMethodHandles() throws Throwable {

        Item item = new Item(); item.i_id = 1;
        Field field = Item.class.getField("i_id");
        MethodHandle h = MethodHandles.lookup().unreflectGetter(field);
        Integer value = (Integer) h.invoke( item );
        IFilter filter = FilterBuilder.getFilter(EQUALS, Integer::compareTo);

        assertTrue(filter.eval( value ));
    }

    @Test
    public void testCreatingFilter() throws Throwable {

        IFilter filter = FilterBuilder.getFilter(EQUALS, Integer::compareTo);

        Row row = new Row( 1, 2, 3, 10L );
        // filter.and
         assertTrue(filter.eval( row.get(0) ));

    }

    @Test
    public void testDataOperationExecution() throws Exception {

        // 3. create an input event and dispatch for execution
        EventRepository eventRepository = EventRepository.get();
        DataOperationExecutor executor = new DataOperationExecutor(eventRepository);

        // executor.Init();

        // CheckoutRequest event = new CheckoutRequest();
        CustomerNewOrderIn event = new CustomerNewOrderIn(1,1,1);

        eventRepository.inputQueue.add(event);

        Thread.sleep(100000);
    }

    @Test
    public void testParameterizedCall() {

        RepositoryFacade facade = new RepositoryFacade( IProductRepository.class );

        Catalog catalog = TestCommon.getDefaultCatalog();
        Analyzer analyzer = new Analyzer( catalog );
        facade.setAnalyzer(analyzer);
        Planner planner = new Planner();
        facade.setPlanner( planner );

        Table table = catalog.getTable("customer");

        CompositeKey key = new CompositeKey(1L,1,1);
        Row row = new Row(1L,1,1,1F,"","",1F,1F);

        table.getPrimaryKeyIndex().upsert(key, row);

        IProductRepository proxyInstance = (IProductRepository) Proxy.newProxyInstance(
                AppTest.class.getClassLoader(),
                new Class[] { IProductRepository.class },
                facade);

        DummyLogic logic = new DummyLogic(proxyInstance);

        CheckoutRequest request = new CheckoutRequest();

        logic.checkoutCart(request);

        assertTrue(true);
    }

}
