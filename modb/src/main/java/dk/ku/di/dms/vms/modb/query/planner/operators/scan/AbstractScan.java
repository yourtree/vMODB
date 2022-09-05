package dk.ku.di.dms.vms.modb.query.planner.operators.scan;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractOperator;

public abstract class AbstractScan extends AbstractOperator  {

    public final AbstractIndex<IKey> index;

    // index of the columns
    public final int[] projectionColumns;

    protected final int[] projectionColumnSize;

    public AbstractScan(int entrySize, AbstractIndex<IKey> index, int[] projectionColumns, int[] projectionColumnSize) {
        super(entrySize);
        this.index = index;
        this.projectionColumns = projectionColumns;
        this.projectionColumnSize = projectionColumnSize;
    }

    @Override
    public AbstractScan asScan(){
        return this;
    }

    protected void append( long address, int[] projectionColumns, int[] columnOffset, int[] valueSizeInBytes) {
        ensureMemoryCapacity();
        this.currentBuffer.append(address, projectionColumns, columnOffset, valueSizeInBytes);
        // move the offset pointer
        this.currentBuffer.reserve(entrySize);
    }

}
