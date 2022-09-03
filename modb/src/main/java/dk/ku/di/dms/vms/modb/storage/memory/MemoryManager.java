package dk.ku.di.dms.vms.modb.storage.memory;

import dk.ku.di.dms.vms.modb.definition.Catalog;
import dk.ku.di.dms.vms.modb.definition.Table;
import jdk.incubator.foreign.MemorySegment;

import java.nio.ByteBuffer;
import java.time.temporal.ValueRange;
import java.util.List;
import java.util.Map;

/**
 *
 * Goal: Abstract the assigning of memory segments to operators
 *
 * Through this class, operators can obtain memory to
 * store the results of their operation
 *
 * Based on a "claim", this class estimates a possible
 * good memory size
 *
 * Operators can repeatably claim to obtain more memory
 * In this case, the claim must be updated accordingly in order
 * to better benefit from this class
 *
 *
 * Get a portion of memory. Partition in small buckets online (per request basis, from the operators)
 * If runs out, allocates direct byte buffer.
 *
 */
public final class MemoryManager {

    /**
     * TODO better to rely on the JVM class
     * {@link sun.nio.ch.Util}
     * #getTemporaryDirectBuffer
     * If this class cannot be used, then use it as inspiration?
     */

    private static final int DEFAULT_KB = 2000; // 4KB -> 4000 bytes

    private Catalog catalog;

    private MemorySegment memorySegment;

    private static Map<Long, MemoryRefNode> memoryClaimedMap;

    // manages the freed memory segments
    // ordered by ascending order
    // this makes finding appropriate segments faster
    private static List<ValueRange> freeBlocks;

    // cache of the memory segment address
    private static long address;

    // the last addressable offset of the memory segment
    private long lastOffset;

    // (number of records in the table, number of records already scanned, number of records pruned)
    // also table identifier and predicate

    // based on this input (and also resource constraints and historical usage of this query predicate),
    // the manager will assign a private memory space

    // a function maps the operation execution to the needs
    // tries to optimize how much memory must be assigned
    // according to historical usage and current usage
    // if predicates are not taken into consideration (only the pruned), the approximation may be very wrong
    // for instance, if a different query does not need so much space, memory waste
    // if only a range has more prunes, then the next claimed is bigger for no reason...
    // but must fulfill for now
    // to optimize, must include the columns
    public static MemoryRefNode claim(int operatorExecId, Table table, int nRecords, int nScanned, int nConditionMet){

        // basic case: operation just started
        if(nScanned == 0){

            ValueRange rangeToAssign;

            // basic case
            if(freeBlocks.size() == 1) {
                ValueRange range = freeBlocks.remove(0);
                ValueRange newRange = ValueRange.of( range.getMinimum() + DEFAULT_KB + Byte.BYTES, range.getMaximum() );
                freeBlocks.add(newRange);
                rangeToAssign = ValueRange.of( range.getMinimum(), range.getMinimum() + DEFAULT_KB );
            } else {
                // find one, otherwise allocate a buffer
                if(freeBlocks.size() == 0){
                    ByteBuffer bb = ByteBuffer.allocateDirect(DEFAULT_KB);
                    long address = MemoryUtils.getByteBufferAddress(bb);
                    rangeToAssign = ValueRange.of( address, DEFAULT_KB );
                } else {
                    // find a block at least as big. since all blocks are at least 2KB
                    // ascending order.... so getting the first is fine
                    rangeToAssign = freeBlocks.remove(0);
                }
            }

            MemoryRefNode mc = new MemoryRefNode(rangeToAssign.getMinimum(), rangeToAssign.getMaximum());
            memoryClaimedMap.put( rangeToAssign.getMinimum(), mc);
//            this.nextFreeOffset += DEFAULT_KB;
            return mc;
        }

        // catalog.getTable()

        // returning a guess is okay for now

        return null;

    }

    public static MemoryRefNode claim(){
        MemoryRefNode mc = new MemoryRefNode(address, DEFAULT_KB);
        return mc;
    }

    public static MemoryRefNode claim(int bytes){
        MemoryRefNode mc = new MemoryRefNode(address, bytes);
        return mc;
    }

    public void unclaim(MemoryRefNode memoryClaimed){



    }

}
