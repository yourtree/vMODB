package dk.ku.di.dms.vms.modb.index.offheap;

import dk.ku.di.dms.vms.modb.schema.key.IKey;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;

import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.Map;

/**
 *
 * Stores records by primary key as a hash table in memory mapped files.
 *
 * It is a clustered index
 *
 * Optimized for OLTP and batch-based transaction workload
 *
 * Rehash is not yet implemented. To circumvent that we could have hierarchies of hash (e.g., second-level, another hash is applied) within the bucket....
 *
 * That means we keep the modified tuples in a separate memory buffer
 * When batch commit occurs, we parallelly commit the records
 * (only the latest versions, overwritten and serialized execution)
 *
 * The parallel occurs at the bucket level. Each thread can take a bucket,
 * look for its respective records (we can also avoid skews [one thread
 * with many records] and useless work [threads spawned to do nothing
 * since there are no records]).
 *
 *
 *
 */
public class BufferedHashtable {

    private static class Header {

        // the next is calculated from the schema size

        static final int SIZE = Byte.BYTES;

        static final byte active = 1;
        static final byte unactive = 0;
    }

    private static class RecordOffset {

        // offset in the buffer
        public int offset;

        // the exact location of the record
        public ByteBuffer buffer;

    }

    // records are all the same size, size computed through schema definition

    // every record has a header
    // indicate whether it is active or deleted

    // each bucket goes into a different file
    // hash --> bytebuffer
    Map<Integer, ByteBuffer> bufferMap;

    // the inactive records are also stored in different files
    // these are smaller files
    // these buffers can be in memory
    Map<Integer, ByteBuffer> bufferDeletedMap;

    private int size;

    private int threshold;

    // default
    private float loadFactor = 0.75f;

    // number of buckets
    private int buckets;

    // on-flight records (modified)
    // can be direct memory, not durable
    // since we can re-execute from the event inputs
    private Map<Integer, ByteBuffer> nonCommittedRows;

    // obtained from schema
    private int recordSize;

    // the number of buffers... the size of data must guide this number
    private int defaultInitialCapacity = 11;

    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    // ow many records does a tpc table has? then becomes input

    /**
     * Using the page size may a good strategy for resizing the buckets
     * When it passes a page size threshold, that means accessing that
     * bucket may incur in page faults, requiring the OS to retrieve from disk
     *
     * How to define the ideal block size for each block? Having so many records in
     * a bucket of the hash may lead to
     */
    public BufferedHashtable(int initSize, int pageSize){

        // must receive a schema

        // must receive a factory to create mapped memory files

        // we also need a buffer for metadata, to make threshold, loadfactor durable, the hash keys, etc

        // this.threshold = (int)Math.min(defaultInitialCapacity * loadFactor, MAX_ARRAY_SIZE + 1);


    }

    private int hash(IKey key){
        return (key.hashCode() & 0x7FFFFFFF) % buckets;
    }

    public void startBatch(){

        // from this point on we start tracking which buffers are modified
        // so we can flush them at the end

    }

    public void commitBatch(){

    }


    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    /**
     *
     * Can we use bloom filters?
     *
     */
    public RecordOffset get(IKey key) {

        // do we have non-committed record for this key?
        int index = hash( key );
        ByteBuffer bufferBucket = nonCommittedRows.get( index );

        bufferBucket.clear();

        ByteBuffer viewOverBb = bufferBucket.duplicate();

        int N = viewOverBb.getInt();

        // iterate over... we could potentially have in-heap index for the bucket
//        int i = 1;
//        boolean found = false;
//        while( i <= N ) {//
//            if(isRecord( key, viewOverBb ))
//        }
//
//        int hash = key.hashCode();
//        int index = (hash & 0x7FFFFFFF) % tab.length;
//        for (Hashtable.Entry<?,?> e = tab[index]; e != null ; e = e.next) {
//            if ((e.hash == hash) && e.key.equals(key)) {
//                return (V)e.value;
//            }
//        }
        return null;
    }

    private boolean isRecord(IKey key, ByteBuffer buffer){

        // ....
        return false;
    }

    // the record must come with active true to avoid copy memory on insert
    public void put(IKey key, ByteBuffer record) {

        int index = hash( key );

        ByteBuffer bufferBucket = nonCommittedRows.get( index );

        // put bufferBucket in correct position

        bufferBucket.put( record );
    }

    public void remove(Object key) {
        // return null;
    }

}
