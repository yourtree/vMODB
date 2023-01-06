package dk.ku.di.dms.vms.sdk.core.example;

import dk.ku.di.dms.vms.modb.api.annotations.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.SynchronousQueue;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.*;

@Microservice("example2")
public class MicroserviceExample2 {

    private int count;

    private BlockingQueue<Integer> blockingQueue;

    public MicroserviceExample2() {
        this.count = 0;
        this.blockingQueue = new LinkedBlockingDeque<>();
    }

    @Inbound(values = {"out1","out2"})
    @Transactional(type=W)
    public void methodExample2(OutputEventExample1 out1, OutputEventExample2 out2) {
        this.count = out1.id + out2.id;
        this.blockingQueue.add(this.count);
        System.out.println("I am microservice 2: out1 "+out1.id+" out2 "+out2.id);
    }

    public int getCount() throws InterruptedException {
        return this.blockingQueue.take();
    }
}
