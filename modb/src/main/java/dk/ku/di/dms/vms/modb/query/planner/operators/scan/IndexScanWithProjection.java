package dk.ku.di.dms.vms.modb.query.planner.operators.scan;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

/**
 * On-flight scanning, filtering, and projection in a single operator.
 * The caller knows the output format.
 * But called does not know how many records are.
 * Header:
 * int - number of rows returned
 * This must be performed by a proper method at the end of the procedure
 * We can have a method called seal or close() in abstract operator
 * It will put the information on header.
 */
public final class IndexScanWithProjection extends AbstractScan {

    public IndexScanWithProjection(
                     Table table,
                     ReadOnlyIndex<IKey> index,
                     int[] projectionColumns,
                     int entrySize) {
        super(table, entrySize, index, projectionColumns);
    }

    @Override
    public boolean isIndexScan() {
        return true;
    }

    @Override
    public IndexScanWithProjection asIndexScan() {
        return this;
    }

    // default call
    public MemoryRefNode run(FilterContext filterContext, IKey... keys) {
        return this.run(this.index, filterContext, keys);
    }

    // transactional call
    public MemoryRefNode run(ReadOnlyIndex<IKey> index, FilterContext filterContext, IKey... keys) {
        // unifying in terms of iterator
        IRecordIterator iterator = index.iterator(keys);
        while(iterator.hasElement()){
            if(index.checkCondition(iterator, filterContext)){
                append(iterator, projectionColumns);
            }
            iterator.next();
        }
        return memoryRefNode;
    }

}