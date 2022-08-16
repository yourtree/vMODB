package dk.ku.di.dms.vms.modb.query.planner.operator.scan;

import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.query.planner.operator.filter.refac.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.operator.result.RowOperatorResult;
import dk.ku.di.dms.vms.modb.query.planner.operator.filter.FilterInfo;
import dk.ku.di.dms.vms.modb.schema.key.IKey;
import dk.ku.di.dms.vms.modb.storage.MemoryManager;

import java.util.Iterator;

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
public final class SequentialScan extends AbstractScan {

    private MemoryManager memoryManager;

    private FilterContext filterContext;

    public SequentialScan(AbstractIndex<IKey> index,
                          FilterInfo filterInfo) {
        super(index, filterInfo);
    }

    public SequentialScan(AbstractIndex<IKey> index) {
        super(index);
    }

    @Override
    public RowOperatorResult get() {

        // https://muratbuffalo.blogspot.com/2022/08/hekaton-sql-servers-memory-optimized.html

//        SequenceLayout SEQUENCE_LAYOUT = MemoryLayout.sequenceLayout(1024, ValueLayout.JAVA_INT);
//        MemorySegment segment = MemorySegment.allocateNative(SEQUENCE_LAYOUT, scope);
//
//
//        int sum = segment.elements(ValueLayout.JAVA_INT).parallel()
//                                            .mapToInt(s -> s.get(ValueLayout.JAVA_INT, 0))
//                                            .sum();

        MemoryManager.MemoryClaimed claimed = memoryManager.claim(0, index.size(), 0, 0);

        // get iterator from index (he knows better how to access its records)
        Iterator<long> iterator = index.iterator();

        // just a matter of managing the indexes of both predicate types

        long address;
        while(iterator.hasNext()){

            address = iterator.next();



        }

        return null;
    }

}
