package dk.ku.di.dms.vms.modb.query.planner.operators.count;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

/**
 * No projecting any other column for now
 *
 * Count is always integer. But can be indexed or not, like the scan
 * If DISTINCT, must maintain state.
 * 
 */
public class IndexCount extends AbstractCount {

    public IndexCount(ReadOnlyIndex<IKey> index) {
        super(index, Integer.BYTES);
    }

    public MemoryRefNode run(FilterContext filterContext, IKey... keys){

        int count = 0;

        IRecordIterator iterator = this.index.iterator(keys);
        while(iterator.hasElement()){
            if(index.checkCondition(iterator, filterContext)){
                count++;
            }
            iterator.next();
        }

        append(count);
        return memoryRefNode;

    }

}
