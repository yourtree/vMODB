package dk.ku.di.dms.vms.modb.query.execution.operators.scan;

import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.query.execution.operators.AbstractSimpleOperator;

import java.util.Iterator;

public abstract class AbstractScan extends AbstractSimpleOperator {

    public final ReadWriteIndex<IKey> index;

    // index of the columns
    public final int[] projectionColumns;

    public AbstractScan(int entrySize, ReadWriteIndex<IKey> index, int[] projectionColumns) {
        super(entrySize);
        this.index = index;
        this.projectionColumns = projectionColumns;
    }

    @Override
    public AbstractScan asScan(){
        return this;
    }

    protected void append(Iterator<IKey> iterator, int[] projectionColumns) {
        ensureMemoryCapacity();
        Object[] record = index.record(iterator);
        for (int projectionColumn : projectionColumns) {
            DataTypeUtils.callWriteFunction(this.currentBuffer.address(), index.schema().columnDataType(projectionColumn), record[projectionColumn]);
            this.currentBuffer.forwardOffset(index.schema().columnDataType(projectionColumn).value);
        }
    }

}
