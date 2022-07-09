package dk.ku.di.dms.vms.modb.query.planner.operator.insert;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

public class Seek implements OneToOneProducer<ByteBuffer>, ContinuousStreamOperator, CloseableOperator, Runnable {

    private BlockingQueue<ByteBuffer> inputQueue;
    private BlockingQueue<ByteBuffer> outputQueue;

    @Override
    public void run() {

    }

    @Override
    public void accept(ByteBuffer buffer) {
        this.inputQueue.add( buffer );
    }

    @Override
    public void signalEndOfStream() {

    }

    @Override
    public ByteBuffer getNext() {
        try {
            return this.outputQueue.take();
        } catch (InterruptedException e) {
            return null;
        }
    }
}
