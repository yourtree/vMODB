package dk.ku.di.dms.vms.coordinator.coordinator;

import dk.ku.di.dms.vms.coordinator.server.coordinator.batch.BatchAlgo;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.Coordinator;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.IVmsWorker;
import dk.ku.di.dms.vms.coordinator.server.coordinator.runnable.VmsIdentifier;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionBootstrap;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionDAG;
import dk.ku.di.dms.vms.modb.common.schema.network.meta.NetworkAddress;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.web_common.runnable.StoppableRunnable;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
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
        VmsIdentifier vms1 = new VmsIdentifier( new VmsNode("",0,"vms1",1,1,0,null,null,null), null);
        VmsIdentifier vms2 = new VmsIdentifier( new VmsNode("",0,"vms2",2,2,1,null,null,null), null);

        Map<String, VmsIdentifier> vmsMetadataMap = new HashMap<>(2);
        vmsMetadataMap.put(vms1.getIdentifier(), vms1);
        vmsMetadataMap.put(vms2.getIdentifier(), vms2);

        // build DAG
        TransactionDAG dag = TransactionBootstrap.name("test").input("a", "vms1", "input1").terminal("b","vms2","a").build();

        Map<String, Long> dependenceMap = BatchAlgo.buildPrecedenceMap( dag.inputEvents.get("input1"), dag, vmsMetadataMap );

        assert dependenceMap.get("vms1") == 1 && dependenceMap.get("vms2") == 2;

    }
    @Test
    public void testComplexDependenceMap(){

        // build VMSs
        VmsIdentifier vms1 = new VmsIdentifier( new VmsNode("",0,"customer",1,1,0,null,null,null), null);
        VmsIdentifier vms2 = new VmsIdentifier( new VmsNode("",0,"item",2,2,1,null,null,null), null);
        VmsIdentifier vms3 = new VmsIdentifier( new VmsNode("",0,"stock",3,3,2,null,null,null), null);
        VmsIdentifier vms4 = new VmsIdentifier( new VmsNode("",0,"warehouse",4,4,3,null,null,null), null);
        VmsIdentifier vms5 = new VmsIdentifier( new VmsNode("",0,"order",5,5,4,null,null,null), null);

        Map<String, VmsIdentifier> vmsMetadataMap = new HashMap<>(5);
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

        Map<String, Long> dependenceMap = BatchAlgo.buildPrecedenceMap( dag, vmsMetadataMap );

        assert dependenceMap.get("customer") == 1 && dependenceMap.get("item") == 2 && dependenceMap.get("stock") == 3
                && dependenceMap.get("warehouse") == 4 && dependenceMap.get("order") == 5;
    }

    // test correctness of a batch... are all dependence maps correct?

    /**
     * test the batch protocol. with a simple dag, 1 source, one terminal
     *
     */
    @Test
    public void testBasicCommitProtocol(){

        // need a transaction dag and corresponding VMSs
        // need a producer of transaction inputs (a separate thread or this thread)
        // need the coordinator to assemble the batch
        // need vms workers
        // no need of a scheduler

        // need of custom VMSs to respond to the batch protocol correctly



        // it would be nice to decouple the network from the batch algorithm...



    }

    /**
     * What should these test vms workers do?
     *
     */
    private static final class TestVmsWorker extends StoppableRunnable implements IVmsWorker {

        BlockingQueue<Coordinator.Message> coordinatorQueue;

        // indicates if it needs to send batch complete message
        boolean terminal;
        public TestVmsWorker(BlockingQueue<Coordinator.Message> coordinatorQueue, boolean terminal){
            this.coordinatorQueue = coordinatorQueue;
            this.terminal = terminal;
        }

        @Override
        public void run() {
            while (this.isRunning()){
                try {
                    Message workerMessage = this.queue().take();
                    switch (workerMessage.type()){
                        // in order of probability
                        case SEND_BATCH_OF_EVENTS -> {
                            logger.info("do something");
                        }
                        case SEND_BATCH_OF_EVENTS_WITH_COMMIT_INFO -> {
                            logger.info("do something");
                        }
                        case SEND_BATCH_COMMIT_COMMAND -> {
                            logger.info("do something");
                        }
                        // case SEND_TRANSACTION_ABORT -> this.sendTransactionAbort(workerMessage);
                        // case SEND_CONSUMER_SET -> this.sendConsumerSet(workerMessage);
                    }
                } catch (InterruptedException e) {
                    logger.warning("This thread has been interrupted. Cause: "+e.getMessage());
                    this.stop();
                }
            }
        }

        @Override
        public BlockingDeque<TransactionEvent.Payload> transactionEventsPerBatch(long batch) {
            return null;
        }

        @Override
        public BlockingQueue<Message> queue() {
            return null;
        }
    }

    // with a source, an internal, and a terminal

    // a source and two terminals

    // two sources, a terminal

}
