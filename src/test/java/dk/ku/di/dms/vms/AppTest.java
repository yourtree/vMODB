package dk.ku.di.dms.vms;

import static dk.ku.di.dms.vms.database.query.parser.enums.ExpressionEnum.EQUALS;
import static org.junit.Assert.assertTrue;

import dk.ku.di.dms.vms.database.api.modb.RepositoryFacade;
import dk.ku.di.dms.vms.database.query.planner.node.filter.Filter;
import dk.ku.di.dms.vms.database.query.planner.node.filter.FilterBuilder;
import dk.ku.di.dms.vms.database.query.planner.node.filter.IFilter;
import dk.ku.di.dms.vms.database.store.refac.Row;
import dk.ku.di.dms.vms.operational.DataOperationExecutor;
import dk.ku.di.dms.vms.event.EventRepository;
import dk.ku.di.dms.vms.eShopOnContainers.events.AddProductRequest;
import dk.ku.di.dms.vms.eShopOnContainers.logic.LogicDummyTest;
import dk.ku.di.dms.vms.eShopOnContainers.repository.IProductRepository;
import dk.ku.di.dms.vms.tpcc.entity.Item;
import dk.ku.di.dms.vms.tpcc.events.CustomerNewOrderIn;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;

public class AppTest 
{
    private static Logger log = LoggerFactory.getLogger(AppTest.class);

    @Test
    public void testDynamicProxying() {
        IProductRepository proxyInstance = (IProductRepository) Proxy.newProxyInstance(
                AppTest.class.getClassLoader(),
                new Class[] { IProductRepository.class },
                new RepositoryFacade( IProductRepository.class ));

        LogicDummyTest logic = new LogicDummyTest(proxyInstance);

        AddProductRequest request = new AddProductRequest();

        logic.addProduct(request);

        assertTrue(true);
    }

    @Test
    public void testCreatingFilter() throws Throwable {

        Item item = new Item(); item.i_id = 1;
        Field field = Item.class.getField("i_id");
        MethodHandle h = MethodHandles.lookup().unreflectGetter(field);
        Integer value = (Integer) h.invoke( item );
        // IFilter<?> filter = FilterBuilder.getFilter(EQUALS, 1);

        // FIXME
        Row row = null;
        // filter.and
        // assertTrue(filter.test( row ));
        assert true;
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

//    @Test
//    public void testExport() {
//
//        try {
//            HibernateExporter exporter =
//                    new HibernateExporter("org.hibernate.dialect.H2Dialect", "dk.ku.di.dms.vms");
//            exporter.exportToConsole();
//            exporter.export(new File("schema.sql"),true,false);
//        }
//        catch(Exception e){
//            System.out.println(e.getMessage());
//        }
//
//    }

}
