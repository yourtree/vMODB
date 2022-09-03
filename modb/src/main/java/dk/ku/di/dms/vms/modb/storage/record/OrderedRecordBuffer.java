package dk.ku.di.dms.vms.modb.storage.record;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryRefNode;
import sun.misc.Unsafe;

import java.util.LinkedList;

import static dk.ku.di.dms.vms.modb.definition.Header.active;
import static dk.ku.di.dms.vms.modb.definition.Header.inactive;

/**
 * A double-linked list maintained
 * in a buffer.
 *
 * Record-aware, meaning it requires knowing
 * the column of the schema that form the index
 *
 */
public class OrderedRecordBuffer {

    private static final Unsafe UNSAFE = MemoryUtils.UNSAFE;

    private final LinkedList<Long> deletedOffsets;

    private final AppendOnlyBuffer buffer;

    // entry size :
    public final int entrySize = 1 + Integer.BYTES + (3 * Long.BYTES);

    private long first;
    private long last;
    private long half;

    // excluding the deleted
    private int records;

    public static final int deltaKey = 1 + Long.BYTES;
    public static final int deltaOffset = 1 + Long.BYTES + Integer.BYTES;
    public static final int deltaNext = 1 + Integer.BYTES + (2 * Long.BYTES);
    public static final int deltaPrevious = 1;

    public OrderedRecordBuffer(AppendOnlyBuffer buffer) {
        this.buffer = buffer;
        this.first = buffer.address();
        this.last = buffer.address();
        this.deletedOffsets = new LinkedList<>();
        this.records = 0;
    }

    public void delete(IKey key){

        long targetAddress = existsAddress(key);

        if(targetAddress == -1) return;

        UNSAFE.putBoolean(null, targetAddress, inactive);

        records--;

        deletedOffsets.add(targetAddress);

        // update first, last, half accordingly
        if(records == 0){
            this.first = buffer.address();
            this.last = first;
            this.half = first;
            return;
        }

        // target address cannot be first and last at the same time,
        // otherwise the number of records would be 0 (see above)
        // that is why we have the "else" below
        if(targetAddress == first){
            // then the next is now the first
            this.first = UNSAFE.getLong(targetAddress + deltaNext);
        } else if(targetAddress == last){
            this.last = UNSAFE.getLong(targetAddress + deltaPrevious);

        }

        if(records == 1){
            this.half = this.first; // or last, does not matter
            return;
        }

        if(records == 2){
            this.half = this.last; // half is always the last in case of two records
            return;
        }

        // move the half
        this.half = UNSAFE.getLong(half + deltaPrevious);

    }

    public void bulkInsert(long srcAddress){

        // given the schema and the position of memory of PK index
        // iterate over the records and build the secondary index


    }

    public void update(IKey key, long srcAddress){

        // findPosition
        long targetAddress = existsAddress(key);

        // update srcAddress
        UNSAFE.putLong( targetAddress + deltaOffset, srcAddress );

    }

    public void insert(IKey key, long srcAddress){

        // the records may be very far from each other
        // a vacuum procedure may be invoked from time to time
        // to move the records, so they are clustered in memory,
        // avoiding paging and improving performance

        long destOffset;

        // get destination offset
        if(!deletedOffsets.isEmpty()){

            destOffset = deletedOffsets.removeLast();

        } else {

            destOffset = buffer.nextOffset();

            // increasing the "position" in the buffer
            buffer.reserve(entrySize);

        }

        // set active
        UNSAFE.putBoolean( null, destOffset, active);
        this.records++;

        if(records == 0){
            // then it is not possible to build the key from target address

            // put in place
            putRecordInfo( key.hashCode(), destOffset, srcAddress );
            half = destOffset;
            return;
        }

        long targetAddress = findPositionToInsert(key);

        // get the SK of the record stored in the target address
        int targetKey = UNSAFE.getInt(targetAddress + deltaKey);

        if(key.hashCode() > targetKey){
            putAfter(targetAddress, destOffset);

            // walk half forwards
            half = UNSAFE.getLong(half + deltaNext);

        } else {
            putBefore(targetAddress, destOffset);

            // walk half forwards
            half = UNSAFE.getLong(half + deltaPrevious);

        }

        // finish writing record info ( key + srcAddress )
        putRecordInfo( targetKey, destOffset, srcAddress );

    }

    private void putRecordInfo(int key, long destOffset, long srcAddress){
        // overwrite the key
        UNSAFE.putInt( destOffset + deltaKey, key );
        // overwrite the srcAddress
        UNSAFE.putLong( destOffset + deltaOffset, srcAddress );
    }

    private void putBefore(long targetAddress, long destOffset) {
        // get the previous of the target address node
        long auxPrevious = UNSAFE.getLong( targetAddress + deltaPrevious );

        // point the "previous" of the target address node to the new record
        UNSAFE.putLong( targetAddress + deltaPrevious, destOffset );

        // update the previous to auxPrevious
        UNSAFE.putLong( destOffset + deltaPrevious, auxPrevious );

        if(first == targetAddress){
            first = destOffset;
        }
    }

    private void putAfter(long targetAddress, long destOffset) {

        // get the next of the target address node
        long auxNext = UNSAFE.getLong(targetAddress + deltaNext);

        // point the "next" of the target address node to the new record
        UNSAFE.putLong(targetAddress + deltaNext, destOffset);

        // update the next to auxNext
        UNSAFE.putLong(destOffset + deltaNext, auxNext);

        if(last == targetAddress){
            last = destOffset;
        }

    }

    /**
     * Cannot apply binary search
     * Records are not sequentially placed in the memory
     * Moving records across addresses on every write/delete
     * may be expensive, and cost increase as more records
     * are found.
     *
     * The optimization possible is keeping two pointers
     * One starts at the first and goes forward
     * The other starts at the last record and goes backwards
     *
     * Best case: O(1) -> the first or the last
     * Worst case: O(n/2) -> the half of the list
     * Average case: < O(n/2)? depend on the workload
     *
     * How to optimize this?
     * Maintain the half record.
     * The half moves for every insertion and deletion
     * The find position starts with the half
     *
     * Guaranteed worst case: O(n/4)...
     *
     */
    private long findPositionToInsert(IKey key){

        if(records == 0) return first;

        if (first == last) return first;

        long bottomAddress;
        long upAddress;
        int bottomKey;
        int upKey;

        // start from half
        int halfKey = UNSAFE.getInt(half + deltaKey);

        if (key.hashCode() > halfKey) {

            // get the next of the half node
            bottomAddress = UNSAFE.getLong(half + deltaNext);
            upAddress = last;

        } else {

            bottomAddress = first;

            // get the previous of the half node
            upAddress = UNSAFE.getLong(half + deltaPrevious);

        }

        do {

            bottomKey = UNSAFE.getInt(bottomAddress + deltaKey);

            if(key.hashCode() > bottomKey){
                bottomAddress = UNSAFE.getLong( bottomAddress + deltaNext );

            } else {
                return bottomAddress;
            }

            upKey = UNSAFE.getInt(upAddress + deltaKey);

            if(key.hashCode() < upKey){
                upAddress = UNSAFE.getLong( upAddress + deltaPrevious );
            } else {
                return upAddress;
            }

        } while(bottomAddress != upAddress); // both arrived at the "half" of the list

        // return any
        return bottomAddress;

    }

    // same logic, but returns an address
    // -1 if does not exist
    public long existsAddress(IKey key){
        // majority of cases
        if(records > 2) {

            long bottomAddress;
            long upAddress;
            int bottomKey;
            int upKey;

            // start from half
            int halfKey = UNSAFE.getInt(half + deltaKey);

            if (key.hashCode() > halfKey) {

                // get the next of the half node
                bottomAddress = UNSAFE.getLong(half + deltaNext);
                upAddress = last;

            } else {

                bottomAddress = first;

                // get the previous of the half node
                upAddress = UNSAFE.getLong(half + deltaPrevious);

            }

            do {

                bottomKey = UNSAFE.getInt(bottomAddress + deltaKey);

                if (key.hashCode() == bottomKey) {
                    return bottomAddress;
                } else {
                    bottomAddress = UNSAFE.getLong(bottomAddress + deltaNext);
                }

                upKey = UNSAFE.getInt(upAddress + deltaKey);

                if (key.hashCode() == upKey) {
                    return upAddress;
                } else {
                    upAddress = UNSAFE.getLong(upAddress + deltaPrevious);
                }

            } while (bottomAddress != upAddress);

            // should make a read. the bottom is pointing to the previous "bottom"
            int mediumKey = UNSAFE.getInt(bottomAddress + deltaKey);

            return mediumKey == key.hashCode() ? bottomAddress : -1;
        }

        if(records == 0){
            return -1;
        }
        if(records == 1){
            int firstKey = UNSAFE.getInt(first + deltaKey);
            return firstKey == key.hashCode() ? first : -1;
        }
        if(records == 2){
            int firstKey = UNSAFE.getInt(first + deltaKey);
            int lastKey = UNSAFE.getInt(last + deltaKey);
            long res = firstKey == key.hashCode() ? first : -1;
            if(res == -1){
                return lastKey == key.hashCode() ? last : -1;
            }
            return res;
        }

        throw new IllegalStateException("Cannot have negative number of records");
    }

    public boolean exists(IKey key){
        long address = existsAddress(key);
        return address != -1;
    }

    /**
     * Minimum swap algorithm:
     * https://www.hackerrank.com/challenges/minimum-swaps-2
     *
     * This is potentially performed in a batch commit
     * to alleviate the overhead of cache misses
     * for the next transactions
     *
     * Strategy:
     * - start in the first bucket pos
     * - find the minimum record
     * - swap the minimum record with the record in the first bucket pos
     *
     * repeat until all buckets are covered (i.e., ordered)
     *
     * This can be optimized compared to the array version since we have a half
     * which allow us to cut in half the cost of finding the next minimum element
     *
     */
    private void reorder(){

        long addressToSwap = this.buffer.address();
        long nextElemToSwap = this.first;

        MemoryRefNode mc = MemoryManager.claim(entrySize);

        // move along the linked list. if the next is not the next address, then swap
        int activeRecordsVerified = 0;
        while(activeRecordsVerified < records){

            // while the next element is inactive
            while(true){
                boolean active = UNSAFE.getBoolean(null, nextElemToSwap);
                if(active) {
                    break;
                }
                nextElemToSwap = UNSAFE.getLong( nextElemToSwap + deltaNext );
            }

            if(addressToSwap != nextElemToSwap){

                // if the addressToSwap is not active, no need to copy
                boolean active = UNSAFE.getBoolean(null, addressToSwap);

                if(active){
                    // copy first to auxiliary memory segment
                    UNSAFE.copyMemory( nextElemToSwap, mc.address(), entrySize );
                    // move the current address to the swapped elem address
                    UNSAFE.copyMemory( addressToSwap, nextElemToSwap, entrySize );
                    // bring the nextElemToSwap to the correct position
                    UNSAFE.copyMemory( mc.address(), addressToSwap, entrySize );
                } else {
                    // just overwrite
                    UNSAFE.copyMemory(nextElemToSwap, addressToSwap, entrySize);
                }

                // can already read from the new address
                nextElemToSwap = UNSAFE.getLong(addressToSwap+deltaNext);

                // next (physical) address
                addressToSwap += entrySize;
            }

            activeRecordsVerified++;

        }

        // at the end, all deleted offsets will be used
        deletedOffsets.clear();

    }

    public long address(){
        return this.buffer.address();
    }

    public int size(){
        return this.records;
    }

    public long getFirst() {
        return first;
    }

    // active (boolean) -> bit whether the record is active
    // previous (long) ->  the address of the previous record (in this buffer)
    // SK (int) -> SK to avoid rebuilding the sk on every iteration (trade-off to speed-up search, but a duplicate info)
    // ordered by the SK... does not mean all the Sk in this bucket belongs to the same sk... implement conflict-free otherwise will be dependent on the input
    // srcAddress (long) -> the src address of the record in the PK index
    // next (long) -> the address of the next record (in this buffer)
    //    private static class Entry {
    //        public boolean active;
    //        public long previous;
    //        public int key;
    //        public long srcAddress;
    //        public long next;
    //    }

}
