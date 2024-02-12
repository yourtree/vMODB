package dk.ku.di.dms.vms.modb.query.execution.operators.scan;

import dk.ku.di.dms.vms.modb.query.execution.operators.AbstractSimpleOperator;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

public abstract class AbstractScan extends AbstractSimpleOperator {

    protected final IMultiVersionIndex index;

    // index of the columns
    private final int[] projectionColumns;

    public AbstractScan(int entrySize, IMultiVersionIndex index, int[] projectionColumns) {
        super(entrySize);
        this.index = index;
        this.projectionColumns = projectionColumns;
    }

//    @Override
//    public AbstractScan asScan(){
//        return this;
//    }

//    protected void append(Iterator<IKey> iterator, int[] projectionColumns) {
////        ensureMemoryCapacity();
//        Object[] record = index.record(iterator);
//        for (int projectionColumn : projectionColumns) {
//            DataTypeUtils.callWriteFunction(this.currentBuffer.address(), index.schema().columnDataType(projectionColumn), record[projectionColumn]);
//            this.currentBuffer.forwardOffset(index.schema().columnDataType(projectionColumn).value);
//        }
//    }

    public IMultiVersionIndex index(){
        return this.index;
    }

}
