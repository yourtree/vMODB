package dk.ku.di.dms.vms.modb.query.planner.operators.scan;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractOperator;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordIterator;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryRefNode;

public class ScanWithProjection extends AbstractOperator {

    private final AbstractIndex<IKey> index;

    // index of the columns
    private final int[] projectionColumns;

    private final int[] projectionColumnSize;

    public ScanWithProjection(AbstractIndex<IKey> index,
                                   int[] projectionColumns,
                                   int[] projectionColumnSize, // in bytes
                                   int entrySize) {
        super(entrySize);
        this.projectionColumns = projectionColumns;
        this.projectionColumnSize = projectionColumnSize;
        this.index = index;
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

}
