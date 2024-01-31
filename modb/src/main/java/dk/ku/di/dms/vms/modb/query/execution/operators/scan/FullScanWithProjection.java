package dk.ku.di.dms.vms.modb.query.execution.operators.scan;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;

import java.util.Iterator;

public class FullScanWithProjection extends AbstractScan {

    public FullScanWithProjection(ReadWriteIndex<IKey> index,
                                  int[] projectionColumns,
                                  int entrySize) {
        super(entrySize, index, projectionColumns);
    }

    public MemoryRefNode run(FilterContext filterContext){
        return this.run(this.index, filterContext);
    }

    public MemoryRefNode run(ReadOnlyIndex<IKey> index, FilterContext filterContext){
        Iterator<IKey> iterator = index.iterator();
        while(iterator.hasNext()){
            if(index.checkCondition(iterator, filterContext)){
                append(iterator, projectionColumns);
            }
            iterator.next();
        }
        return memoryRefNode;
    }

    @Override
    public boolean isFullScan() {
        return true;
    }

    @Override
    public FullScanWithProjection asFullScan() {
        return this;
    }

}
