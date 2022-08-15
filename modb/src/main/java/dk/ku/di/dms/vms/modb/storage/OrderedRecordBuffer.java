package dk.ku.di.dms.vms.modb.storage;

import dk.ku.di.dms.vms.modb.schema.Schema;
import dk.ku.di.dms.vms.modb.schema.key.IKey;
import dk.ku.di.dms.vms.modb.schema.key.KeyUtils;
import sun.misc.Unsafe;

import java.util.LinkedList;

import static dk.ku.di.dms.vms.modb.schema.Header.active;
import static dk.ku.di.dms.vms.modb.schema.Header.inactive;

/**
 * A double-linked list maintained
 * in a buffer
 */
public class OrderedRecordBuffer {

    private static final Unsafe UNSAFE = MemoryUtils.UNSAFE;

    private final LinkedList<Long> deletedOffsets;

    private final AppendOnlyBuffer buffer;

    // active (byte) -> bit whether the record is active
    // previous ->  the src address of the previous record in this buffer
    // srcAddress (long) -> the src address of the record in the PK index
    // next -> the src address of the next record in this buffer

    // entry size :
    private final int entrySize = Byte.BYTES + (3 * Long.BYTES);

    private long first; // = buffer.address();
    private long last; // = buffer.address();
    private long half;

    private final Schema schema;

    // excluding the deleted
    private int records;

    private final int deltaNext = Byte.BYTES + (2 * Long.BYTES);
    private final int deltaPrevious = Byte.BYTES;

    public OrderedRecordBuffer(AppendOnlyBuffer buffer, Schema schema) {
        this.buffer = buffer;
        this.schema = schema;
        this.first = buffer.address();
        this.last = buffer.address();
        this.deletedOffsets = new LinkedList<>();
        this.records = 0;
    }

    public void delete(IKey key){

        long targetAddress = existsAddress(key);

        if(targetAddress == -1) return;

        UNSAFE.putByte(targetAddress, inactive);

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

    public void insert(IKey key, long srcAddress){

        // the records may be very far from each other
        // a vacuum procedure may be invoked from time to time
        // to move the records, so they are clustered in memory,
        // avoiding paging and improving performance

        long targetAddress = findPositionToInsert(key);

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
        UNSAFE.putByte( destOffset, active);
        this.records++;

        if(records == 0){
            // then it is not possible to build the key from target address
            // besides, half does not move

            // put in place
            UNSAFE.putLong(destOffset + Byte.BYTES + Long.BYTES, srcAddress);

            return;
        }

        // build the PK of the record in the target address
        IKey targetKey = KeyUtils.buildRecordKey(schema,
                schema.getPrimaryKeyColumns(), targetAddress);

        if(key.hashCode() > targetKey.hashCode()){
            putAfter(srcAddress, targetAddress, destOffset);

            // walk half forwards
            half = UNSAFE.getLong(half + deltaNext);

        } else {
            putBefore(srcAddress, targetAddress, destOffset);

            // walk half forwards
            half = UNSAFE.getLong(half + deltaPrevious);

        }

    }

    private void putBefore(long srcAddress, long targetAddress, long destOffset) {
        // get the previous of the target address node
        long auxPrevious = UNSAFE.getLong( targetAddress + deltaPrevious );

        // point the "previous" of the target address node to the new record
        UNSAFE.putLong( targetAddress + deltaPrevious, destOffset );

        // overwrite the srcAddress
        UNSAFE.putLong( destOffset + Byte.BYTES + Long.BYTES, srcAddress );

        // update the previous to auxPrevious
        UNSAFE.putLong( destOffset + deltaPrevious, auxPrevious );

        if(first == targetAddress){
            first = destOffset;
        }
    }

    private void putAfter(long srcAddress, long targetAddress, long destOffset) {

        // get the next of the target address node
        long auxNext = UNSAFE.getLong(targetAddress + deltaNext);

        // point the "next" of the target address node to the new record
        UNSAFE.putLong(targetAddress + deltaNext, destOffset);

        // overwrite the srcAddress
        UNSAFE.putLong(destOffset + Byte.BYTES + Long.BYTES, srcAddress);

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

        if(records > 0) {

            if (first == last) {
                return first;
            }

            long bottomAddress;
            long upAddress;
            IKey bottomKey;
            IKey upKey;

            // start from half
            IKey halfKey = KeyUtils.buildRecordKey(schema,
                    schema.getPrimaryKeyColumns(), half);

            if (key.hashCode() > halfKey.hashCode()) {

                // get the next of the half node
                bottomAddress = UNSAFE.getLong(half + deltaNext);
                upAddress = last;

            } else {

                bottomAddress = first;

                // get the previous of the half node
                upAddress = UNSAFE.getLong(half + deltaPrevious);

            }

            do {

                bottomKey = KeyUtils.buildRecordKey(schema,
                        schema.getPrimaryKeyColumns(), bottomAddress);

                if(key.hashCode() > bottomKey.hashCode()){
                    bottomAddress = UNSAFE.getLong( bottomAddress + deltaNext );

                } else {
                    return bottomAddress;
                }

                upKey = KeyUtils.buildRecordKey(schema,
                        schema.getPrimaryKeyColumns(), upAddress);

                if(key.hashCode() < upKey.hashCode()){
                    upAddress = UNSAFE.getLong( upAddress + deltaPrevious );
                } else {
                    return upAddress;
                }

            } while(bottomAddress != upAddress); // both arrived at the "half" of the list

            // return any
            return bottomAddress;

        }

        return first;// or last

    }

    // same logic, but returns an address
    // -1 if does not exist
    public long existsAddress(IKey key){
        // majority of cases
        if(records > 2) {

            long bottomAddress;
            long upAddress;
            IKey bottomKey;
            IKey upKey;

            // start from half
            IKey halfKey = KeyUtils.buildRecordKey(schema,
                    schema.getPrimaryKeyColumns(), half);

            if (key.hashCode() > halfKey.hashCode()) {

                // get the next of the half node
                bottomAddress = UNSAFE.getLong(half + deltaNext);
                upAddress = last;

            } else {

                bottomAddress = first;

                // get the previous of the half node
                upAddress = UNSAFE.getLong(half + deltaPrevious);

            }

            do {

                bottomKey = KeyUtils.buildRecordKey(schema,
                        schema.getPrimaryKeyColumns(), bottomAddress);

                if (key.hashCode() == bottomKey.hashCode()) {
                    return bottomAddress;
                } else {
                    bottomAddress = UNSAFE.getLong(bottomAddress + deltaNext);
                }

                upKey = KeyUtils.buildRecordKey(schema,
                        schema.getPrimaryKeyColumns(), upAddress);

                if (key.hashCode() == upKey.hashCode()) {
                    return upAddress;
                } else {
                    upAddress = UNSAFE.getLong(upAddress + deltaPrevious);
                }

            } while (bottomAddress != upAddress);

            IKey mediumKey = KeyUtils.buildRecordKey(schema,
                    schema.getPrimaryKeyColumns(), bottomAddress);

            return mediumKey.hashCode() == key.hashCode() ? bottomAddress : -1;
        }

        if(records == 0){
            return -1;
        }
        if(records == 1){
            IKey firstKey = KeyUtils.buildRecordKey(schema,
                    schema.getPrimaryKeyColumns(), first);
            return firstKey.hashCode() == key.hashCode() ? first : -1;
        }
        if(records == 2){
            IKey firstKey = KeyUtils.buildRecordKey(schema,
                    schema.getPrimaryKeyColumns(), first);
            IKey lastKey = KeyUtils.buildRecordKey(schema,
                    schema.getPrimaryKeyColumns(), last);
            long res = firstKey.hashCode() == key.hashCode() ? first : -1;
            if(res == -1){
                return lastKey.hashCode() == key.hashCode() ? last : -1;
            }
            return res;
        }

        throw new IllegalStateException("Cannot have negative number of records");
    }

    public boolean exists(IKey key){
        long address = existsAddress(key);
        return address != -1;
    }

}
