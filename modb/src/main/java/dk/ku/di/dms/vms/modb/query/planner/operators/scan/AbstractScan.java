package dk.ku.di.dms.vms.modb.query.planner.operators.scan;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractOperator;

public abstract class AbstractScan extends AbstractOperator  {

    public final AbstractIndex<IKey> index;

    // index of the columns
    protected final int[] projectionColumns;

    protected final int[] projectionColumnSize;

    public AbstractScan(int entrySize, AbstractIndex<IKey> index, int[] projectionColumns, int[] projectionColumnSize) {
        super(entrySize);
        this.index = index;
        this.projectionColumns = projectionColumns;
        this.projectionColumnSize = projectionColumnSize;
    }

    public AbstractScan asAbstractScan(){
        return this;
    }

}
