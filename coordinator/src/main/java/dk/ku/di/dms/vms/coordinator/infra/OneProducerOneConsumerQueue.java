package dk.ku.di.dms.vms.coordinator.infra;

import java.util.Collection;

/**
 * This class is only safe when used by a single producer and a single consumer
 * If multiple, synchronization must be provided outside of this class
 * @param <T> The type being stored
 */
public class OneProducerOneConsumerQueue<T> implements SimpleQueue<T> {

    private static class ElementBucket<T> {
        public final T[] elementData;
        public volatile int tail;
        public ElementBucket<T> next;

        @SuppressWarnings("unchecked")
        public ElementBucket(){
            this.elementData = (T[]) new Object[DEFAULT_CAPACITY];
            this.tail = 0;
            this.next = null;
        }
    }

    private ElementBucket<T> currentReadBucket;

    private ElementBucket<T> currentWriteBucket;

    /**
     * Default initial capacity.
     */
    private static final int DEFAULT_CAPACITY = 1000;

    /**
     * Must be volatile because the JVM may pin this thread to another CPU
     * Which would refrain the thread from fetching the latest value written
     */
    private int head;

    public OneProducerOneConsumerQueue(){
        this.head = 0;
        this.currentReadBucket = new ElementBucket<>();
        this.currentWriteBucket = this.currentReadBucket;
    }

    // how to never block the reader even in cases where we need to grow the array?
    // let the reader consume the entire old array

    public T remove(){
        // consumer has consumed everything up to now
        if( // currentReadBucket == currentWriteBucket &&
                head == currentReadBucket.tail
                && currentReadBucket.tail < DEFAULT_CAPACITY-1 // at least one to add
        ) {
            // reader must wait for writer
            return null;
        }

        // write bucket is ahead and reader has finished reading the current bucket
        // assuming capacity never changes...
        // if the capacity changes for each bucket, must have the capacity in the element bucket DT
        if(head == DEFAULT_CAPACITY){
            if(currentReadBucket.next != null) {
                currentReadBucket = currentReadBucket.next;
                head = 0;

                // tail is maintained by the writer and only refers to the

                // new buffer is empty?
                if(head == currentReadBucket.tail) return null;

            } else {
                return null;
            }
        }

        head++;
        return currentReadBucket.elementData[head-1];
    }

    /**
     * The head starts in the first array position
     *
     */
    public void add(T element){

        if(currentWriteBucket.tail == DEFAULT_CAPACITY){
            currentWriteBucket.next = new ElementBucket<>();
            currentWriteBucket = currentWriteBucket.next;
        }

        currentWriteBucket.elementData[currentWriteBucket.tail] = element;
        int i = currentWriteBucket.tail + 1;
        currentWriteBucket.tail = i;
    }

    public void drainTo(Collection<T> list){
        // while there is bucket to read, do it
    }

    // approximate
    public int size(){
        int count = currentReadBucket.tail - head;
        ElementBucket<T> refBucket = currentReadBucket;
        while(refBucket.next != null){
            count += refBucket.tail;
            refBucket = refBucket.next;
        }
        return count;
    }

    public boolean isEmpty(){
        // and there is decidedly elements to read
        int currBucketToRead = currentReadBucket.tail - head;
        if(currBucketToRead > 0) {
            return false;
        }

        // read may have reached the final
        // currBucketToRead == 0
        if(currentReadBucket != currentWriteBucket){
            // ok, writer is ahead
            // but is there elements in the next bucket?
            return currentReadBucket.next.tail > 0; // next always reference something. look the if above
        }

        // currentReadBucket == currentWriteBucket
        // returning true means no new writes have been made since the last read
        return true;
    }

}
