package dk.ku.di.dms.vms.modb.query.planner.operators.scan;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

public class FullScanWithProjection extends AbstractScan {

    public FullScanWithProjection(Table table, ReadOnlyIndex<IKey> index,
                                  int[] projectionColumns,
                                  int entrySize) {
        super(table, entrySize, index, projectionColumns);
    }

    public MemoryRefNode run(FilterContext filterContext){
        return this.run(this.index, filterContext);
    }

    public MemoryRefNode run(ReadOnlyIndex<IKey> index, FilterContext filterContext){
        IRecordIterator iterator = index.iterator();
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
