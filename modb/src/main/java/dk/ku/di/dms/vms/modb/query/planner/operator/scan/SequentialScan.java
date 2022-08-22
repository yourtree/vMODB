package dk.ku.di.dms.vms.modb.query.planner.operator.scan;

import dk.ku.di.dms.vms.modb.common.meta.DataType;
import dk.ku.di.dms.vms.modb.query.planner.operator.filter.refac.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.operator.filter.refac.FilterType;
import dk.ku.di.dms.vms.modb.storage.AppendOnlyBuffer;
import dk.ku.di.dms.vms.modb.storage.DataTypeUtils;
import dk.ku.di.dms.vms.modb.storage.infra.MemoryManager;
import dk.ku.di.dms.vms.modb.storage.TableIterator;

import java.util.LinkedList;
import java.util.List;

/**
 * aka table scan
 * A thread should run the scan
 * This is a blocking implementation, i.e.,
 * the upstream operator must wait for this entire execution to get to work
 *
 * The operator must return a data structure that characterizes
 * the result of the operation.
 *
 * The data structure must contain:
 * - An address space. The address space contains a contiguous memory
 * that contains pointers to the actual records (the PK indexed)
 * - The size of the memory space.
 * - The number of records in the memory segment
 *
 * It is unsure how many records will be touched
 *
 * The sequential scan should only be applied in case
 * the filter prunes a significant amount of records
 * in the absence of an index
 *
 * if the index is hash, only contains the pointers to the records
 * if not, the same
 * but the
 *
 */
public final class SequentialScan {

    // the unique identifier of this operator execution
    private final int id;

    // get iterator from index (he knows better how to access its records)
    private final TableIterator iterator;

    private final MemoryManager memoryManager;

    private final FilterContext filterContext;

    private int pruned;

    private final LinkedList<MemoryManager.MemoryClaimed> memoryClaimedList;

    // how to know how much of the memory claimed was used?
    // we need a contextual info about the processing
//    public record SegmentContext(
//            List<MemoryManager.MemoryClaimed> segments,
//            List<int> used
//    ) {}

    /**
     * The iterator allows the planner to decide whether
     * we have several scans working on independent data partitions.
     *
     * What information do I need in the iterator?
     * - size -> total number of records
     * - table -> what is the table
     * -
     */
    public SequentialScan(int id,
                          TableIterator iterator,
                          FilterContext filterContext,
                          MemoryManager memoryManager) {
        this.id = id;
        this.iterator = iterator;
        this.filterContext = filterContext;
        this.memoryManager = memoryManager;
        this.memoryClaimedList = new LinkedList<>();
    }

    // it must return a set of memory segments
    public List<MemoryManager.MemoryClaimed> get() {

        // https://muratbuffalo.blogspot.com/2022/08/hekaton-sql-servers-memory-optimized.html

        // just a matter of managing the indexes of both predicate types

        // the number of filters to apply
        int filterIdx = 0;

        // the filter index on which a given param (e.g., literals, zero, 1, 'SURNAME', etc) should apply
        int biPredIdx = 0;

        // simple predicates, do not involve input params (i.e, NULL, NOT NULL, EXISTS?, etc)
        int predIdx = 0;

        long address;

        boolean conditionHolds = true;

        while(iterator.hasNext()){

            address = iterator.next();

            // if(claimed.) if write surpass lastOffset then get more memory
            while( conditionHolds && filterIdx < filterContext.filterTypes.length ){

                // no need to read active bit

                // this should go on the filter? probably not
                int columnIndex = filterContext.filterColumns[filterIdx];
                int columnOffset = iterator.table().getSchema().getColumnOffset( columnIndex );
                DataType dataType = iterator.table().getSchema().getColumnDataType( columnIndex );

                Object val = DataTypeUtils.getValue( dataType, address + columnOffset );

                // it is a literal passed to the query
                if(filterContext.filterTypes[filterIdx] == FilterType.BP) {
                    conditionHolds = filterContext.biPredicates[biPredIdx].
                            apply(val, filterContext.biPredicateParams[biPredIdx]);
                    biPredIdx++;
                } else {
                    conditionHolds = filterContext.predicates[predIdx].test( val );
                    predIdx++;
                }

                filterIdx++;
            }

            if(conditionHolds){

                ensureMemoryCapacity();

                // add to the output memory space
                this.currentBuffer.append(address);

            } else {
                pruned++;
                conditionHolds = true; // to prepare for next filter iterations
            }

            filterIdx = 0;

        }

        return memoryClaimedList;
    }

    /**
     * Just abstracts on which memory segment a result will be written to
     *
     * Data structure:
     * - srcAddress (long) -> the src address of the record in the PK index
     *
     */
    private void ensureMemoryCapacity(){

//        SequenceLayout SEQUENCE_LAYOUT = MemoryLayout.sequenceLayout(1024, ValueLayout.JAVA_INT);
//        MemorySegment segment = MemorySegment.allocateNative(SEQUENCE_LAYOUT, scope);
//
//        int sum = segment.elements(ValueLayout.JAVA_INT).parallel()
//                                            .mapToInt(s -> s.get(ValueLayout.JAVA_INT, 0))
//                                            .sum();

        if(currentBuffer.capacity() - currentBuffer.address() > entrySize){
            return;
        }

        // write contextual info to this buffer? no need, let the upstream operator infers

        // else, get a new segment
        MemoryManager.MemoryClaimed claimed = memoryManager.claim(this.id, iterator.table(),
                iterator.size(), iterator.progress(), pruned);

        memoryClaimedList.add(claimed);

        this.currentBuffer = new AppendOnlyBuffer(claimed.address(), claimed.bytes());

    }

    private AppendOnlyBuffer currentBuffer;

    private final int entrySize = Long.BYTES;

}
