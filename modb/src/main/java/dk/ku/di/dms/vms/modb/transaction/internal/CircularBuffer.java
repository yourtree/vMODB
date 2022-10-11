package dk.ku.di.dms.vms.modb.transaction.internal;


import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>This implementation was adapted from WebSocket internal Message Queue class.
 */
public class CircularBuffer {

    private int size;

    private static final int DEFAULT_INITIAL_CAPACITY = 10;

    /**
     * Time where a consumer is expected to use the returned element
     */
    private static final int DEFAULT_EXPIRATION_MS = 6000; // 0.1 minute

    private transient Element[] elements;

    private final AtomicInteger tail = new AtomicInteger();
    private volatile int head = 0;

    private int numberOfCallsConsumerHasWaited = 0;

    private static final int CONSUMER_WAIT_THRESHOLD_TO_GROW = 3;

    public CircularBuffer(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException();
        }
        int s = pow2Size(capacity + 1);
        assert s % 2 == 0 : s;
        elements = new Element[s];
    }

    public CircularBuffer() {
        size = DEFAULT_INITIAL_CAPACITY;
        int s = pow2Size(size + 1);
        assert s % 2 == 0 : s;
        elements = new Element[s];
    }

    /**
     * Should never throw exception because the size is always bounded by the number of pre-allocated elements.
     */
    private void add(Object[] record) throws IOException {

        int h, currentTail, newTail;
        do {
            h = head;
            currentTail = tail.get();
            newTail = (currentTail + 1) & (elements.length - 1);
            if (newTail == h) {
                throw new IOException("Queue full");
            }
        } while (!tail.compareAndSet(currentTail, newTail));
        Element element = new Element();
        element.ts = System.currentTimeMillis();
        element.record = record;
        elements[currentTail] = element;
    }

    public Object[] peek(){

        int currentHead = head;
        Element h = elements[currentHead];

        if(h == null){
            // no element to provide. the consumer has to create and later add to this
            return null;
        }

        long currentMs = System.currentTimeMillis();
        long diff = currentMs - h.ts;
        if (diff < DEFAULT_EXPIRATION_MS) {
            // we need to grow the array to accommodate
            try {
                numberOfCallsConsumerHasWaited++;
                if(numberOfCallsConsumerHasWaited > CONSUMER_WAIT_THRESHOLD_TO_GROW){
                    // grow the array
                    size = size * 2;
                    elements = Arrays.copyOf(elements, size);
                    numberOfCallsConsumerHasWaited = 0;
                } else {
                    // give time for the old consumer to (most probably) finish its operation
                    Thread.sleep(diff + 1000);
                    head = (currentHead + 1) & (elements.length - 1);
                }
            } catch (InterruptedException ignored) { }
        } else {
            h.ts = currentMs;
        }
        return h.record;

    }

    private static class Element {

        private volatile long ts;
        private Object[] record;

    }

    private static int pow2Size(int n) {
        if (n <= 0) {
            return 1;
        } else if (n >= (1 << 30)) {
            return 1 << 30;
        } else {
            return 1 << (32 - Integer.numberOfLeadingZeros(n - 1));
        }
    }

}
