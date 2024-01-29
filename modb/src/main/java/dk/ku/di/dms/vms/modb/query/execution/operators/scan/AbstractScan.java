package dk.ku.di.dms.vms.modb.query.execution.operators.scan;

import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.query.execution.operators.AbstractSimpleOperator;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

public abstract class AbstractScan extends AbstractSimpleOperator {

    // for now used to build dynamically a new run
    public final Table table;

    public final ReadOnlyIndex<IKey> index;

    // index of the columns
    public final int[] projectionColumns;

    public AbstractScan(Table table, int entrySize, ReadOnlyIndex<IKey> index, int[] projectionColumns) {
        super(entrySize);
        this.table = table;
        this.index = index;
        this.projectionColumns = projectionColumns;
    }

    @Override
    public AbstractScan asScan(){
        return this;
    }

    protected void append(IRecordIterator<IKey> iterator, int[] projectionColumns) {
        ensureMemoryCapacity();
        Object[] record = index.record(iterator);
        for (int projectionColumn : projectionColumns) {
            DataTypeUtils.callWriteFunction(this.currentBuffer.address(), index.schema().columnDataType(projectionColumn), record[projectionColumn]);
            this.currentBuffer.forwardOffset(index.schema().columnDataType(projectionColumn).value);
        }
    }

}
