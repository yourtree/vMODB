package dk.ku.di.dms.vms.coordinator.coordinator;

import dk.ku.di.dms.vms.coordinator.server.coordinator.batch.BatchCore;
import dk.ku.di.dms.vms.coordinator.transaction.EventIdentifier;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.VmsIdentifier;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * 1. test starters VMSs with active and non-active VMSs
 * 2. test VMS inactive after the first barrier. what to do with the metadata?
 * 3.
 */
public class CoordinatorTest {

    private static final Logger logger = Logger.getLogger("CoordinatorTest");

    @Test
    public void test(){

        // 1 - thread to generate input transactions
        // 2 - start creating artificial transactions

        // 3 - spawn a vms to receive input and output 2 results
        // https://hackingsaas.substack.com/p/hacking-saas-12-collection-of-data?utm_source=twitter&utm_campaign=auto_share&r=1mrckf

    }

    /**
     * In this test, given a transaction DAG and a set of previous
     * transactions from the participating VMSs, decides the
     * dependence map (map of VMS to corresponding lastTid)
     */
    @Test
    public void testSimpleDependenceMap(){

        // build VMSs
        VmsIdentifier vms1 = new VmsIdentifier("",0,"vms1",1,1,0,null,null,null);
        VmsIdentifier vms2 = new VmsIdentifier("",0,"vms2",2,2,1,null,null,null);

        Map<String,VmsIdentifier> vmsMetadataMap = new HashMap<>(2);
        vmsMetadataMap.put(vms1.getIdentifier(), vms1);
        vmsMetadataMap.put(vms2.getIdentifier(), vms2);

        // build DAG
        TransactionDAG dag = TransactionBootstrap.name("test").input("a", "vms1", "input1").terminal("b","vms2","a").build();

        Map<String, Long> dependenceMap = BatchCore.buildPrecedenceMap( dag.inputEvents.get("input1"), dag, vmsMetadataMap );

        assert dependenceMap.get("vms1") == 1 && dependenceMap.get("vms2") == 2;

    }
    @Test
    public void testComplexDependenceMap(){

        // build VMSs
        VmsIdentifier vms1 = new VmsIdentifier("",0,"customer",1,1,0,null,null,null);
        VmsIdentifier vms2 = new VmsIdentifier("",0,"item",2,2,1,null,null,null);
        VmsIdentifier vms3 = new VmsIdentifier("",0,"stock",3,3,2,null,null,null);
        VmsIdentifier vms4 = new VmsIdentifier("",0,"warehouse",4,4,3,null,null,null);
        VmsIdentifier vms5 = new VmsIdentifier("",0,"order",5,5,4,null,null,null);

        Map<String,VmsIdentifier> vmsMetadataMap = new HashMap<>(2);
        vmsMetadataMap.put(vms1.getIdentifier(), vms1);
        vmsMetadataMap.put(vms2.getIdentifier(), vms2);
        vmsMetadataMap.put(vms3.getIdentifier(), vms3);
        vmsMetadataMap.put(vms4.getIdentifier(), vms4);
        vmsMetadataMap.put(vms5.getIdentifier(), vms5);

        // new order transaction
        TransactionDAG dag =  TransactionBootstrap.name("new-order")
                .input( "a", "customer", "customer-new-order-in" )
                .input("b", "item","item-new-order-in" )
                .input( "c", "stock","stock-new-order-in" )
                .input( "d", "warehouse", "waredist-new-order-in" )
                .internal( "e", "customer","customer-new-order-out",  "a" )
                .internal( "f", "item","item-new-order-out", "b" )
                .internal( "g", "stock", "stock-new-order-out", "c" )
                .internal( "h", "warehouse","waredist-new-order-out", "d" )
                // signals the end of the transaction. However, it does not mean generates an output event
                .terminal("i", "order", "b", "e", "f", "g", "h" )
                .build();

        Map<String, Long> dependenceMap = BatchCore.buildPrecedenceMap( dag, vmsMetadataMap );

        assert dependenceMap.get("customer") == 1 && dependenceMap.get("item") == 2 && dependenceMap.get("stock") == 3
                && dependenceMap.get("warehouse") == 4 && dependenceMap.get("order") == 5;
    }


    // test correctness of a batch... are all dependence maps correct?

    // test the batch protocol. with a simple dag, 1 source, one terminal

    // with a source, an internal, and a terminal

    // a source and two terminals

    // two sources, a terminal

}
