package dk.ku.di.dms.vms.modb.query.planner.operators.scan;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordIterator;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryRefNode;

public class FullScanWithProjection extends AbstractScan {

    public FullScanWithProjection(AbstractIndex<IKey> index,
                                  int[] projectionColumns,
                                  int[] projectionColumnSize, // in bytes
                                  int entrySize) {
        super(entrySize, index, projectionColumns, projectionColumnSize);
    }

    public MemoryRefNode run(FilterContext filterContext){

        RecordIterator iterator = index.asUniqueHashIndex().iterator();
        long address;
        while(iterator.hasNext()){

            address = iterator.next();

            // if there is no condition, no reason to do scan
            if(checkCondition(address, filterContext, index)){
                append(address, projectionColumns, index.getTable().getSchema().columnOffset(), projectionColumnSize);
            }

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
