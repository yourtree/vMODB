package dk.ku.di.dms.vms.modb.query.execution.operators.count;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyBufferIndex;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

/**
 * Not projecting any other column for now.
 * Count is always integer. But can be indexed or not, like the scan
 * If DISTINCT, must maintain state.
 * 
 */
public class IndexCount extends AbstractCount {

    public IndexCount(ReadOnlyBufferIndex<IKey> index) {
        super(index, Integer.BYTES);
    }

    public MemoryRefNode run(FilterContext filterContext, IKey... keys){

        int count = 0;

        IRecordIterator<IKey> iterator = this.index.iterator(keys);
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
