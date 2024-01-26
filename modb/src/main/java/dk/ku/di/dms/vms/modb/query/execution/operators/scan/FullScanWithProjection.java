package dk.ku.di.dms.vms.modb.query.execution.operators.scan;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyBufferIndex;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

public class FullScanWithProjection extends AbstractScan {

    public FullScanWithProjection(Table table, ReadOnlyBufferIndex<IKey> index,
                                  int[] projectionColumns,
                                  int entrySize) {
        super(table, entrySize, index, projectionColumns);
    }

    public MemoryRefNode run(FilterContext filterContext){
        return this.run(this.index, filterContext);
    }

    public MemoryRefNode run(ReadOnlyBufferIndex<IKey> index, FilterContext filterContext){
        IRecordIterator<IKey> iterator = index.iterator();
        while(iterator.hasElement()){
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
