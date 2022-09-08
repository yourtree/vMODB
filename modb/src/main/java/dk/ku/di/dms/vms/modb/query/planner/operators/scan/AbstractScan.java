package dk.ku.di.dms.vms.modb.query.planner.operators.scan;

import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractOperator;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

public abstract class AbstractScan extends AbstractOperator  {

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

    protected void append(IRecordIterator iterator, int[] projectionColumns) {
        ensureMemoryCapacity();

        long address = iterator.current();
        IKey key = iterator.primaryKey();

        //this.currentBuffer.append(address, projectionColumns, columnOffset, valueSizeInBytes);

        for(int i = 0; i < projectionColumns.length; i++){

            // so the idea is getting the address from the index. the index has the correct value
            // since the value may differ from the original record address because of updates
            // the version node has the address
            // maybe the iterator dont need to be passed since it is "given" by the consistentview

            // the operator don't know what version, so better to give the index to do it
            long addr = index.getColumnAddress(iterator);
            int size = index.schema().getColumnDataType(projectionColumns[i]).value;
                    // long or object... better to make everything direct address

            this.currentBuffer.copy(addr, size);

        }

        // move the offset pointer
        this.currentBuffer.reserve(entrySize);
    }

}
