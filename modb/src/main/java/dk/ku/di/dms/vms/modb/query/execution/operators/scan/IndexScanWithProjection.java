package dk.ku.di.dms.vms.modb.query.execution.operators.scan;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;

import java.util.Iterator;

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
                     ReadWriteIndex<IKey> index,
                     int[] projectionColumns,
                     int entrySize) {
        super(entrySize, index, projectionColumns);
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
        Iterator<IKey> iterator = index.iterator(keys);
        while(iterator.hasNext()){
            if(index.checkCondition(iterator, filterContext)){
                this.append(iterator, this.projectionColumns);
            }
            iterator.next();
        }
        return this.memoryRefNode;
    }

    public MemoryRefNode run(IKey... keys) {
        Iterator<IKey> iterator = this.index.iterator(keys);
        while(iterator.hasNext()){
            this.append(iterator, this.projectionColumns);
            iterator.next();
        }
        return this.memoryRefNode;
    }

    public MemoryRefNode run(ReadOnlyIndex<IKey> index, IKey... keys) {
        // unifying in terms of iterator
        Iterator<IKey> iterator = index.iterator(keys);
        while(iterator.hasNext()){
            this.append(iterator, this.projectionColumns);
            iterator.next();
        }
        return this.memoryRefNode;
    }

}