package dk.ku.di.dms.vms.modb.query.complex.planner.operator.scan;

import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.query.refac.FilterContext;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordIterator;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryRefNode;

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
                          AbstractIndex<IKey> index,
                          FilterContext filterContext) {
        super(id, index, filterContext);
    }

    // it must return a set of memory segments
    public MemoryRefNode run() {
        // https://muratbuffalo.blogspot.com/2022/08/hekaton-sql-servers-memory-optimized.html
        // get iterator from index (he knows better how to access its records)
        RecordIterator iterator = index.asUniqueHashIndex().iterator();
        processIterator(iterator);
        return memoryRefNode;
    }

}
