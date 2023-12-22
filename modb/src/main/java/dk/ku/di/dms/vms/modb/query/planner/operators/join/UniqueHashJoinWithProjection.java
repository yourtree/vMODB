package dk.ku.di.dms.vms.modb.query.planner.operators.join;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyBufferIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractSimpleOperator;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

/**
 * Basic form of hash join
 * where both indexes are hashed by the same
 * set of columns
 */
public class UniqueHashJoinWithProjection extends AbstractSimpleOperator {

    public final ReadOnlyBufferIndex<IKey> leftIndex;
    public final ReadOnlyBufferIndex<IKey> rightIndex;

    // index of the columns
    protected final int[] leftProjectionColumns;
    protected final int[] leftProjectionColumnsSize;

    // this is unnecessary, since it can be get from the index directly
    protected final int[] rightProjectionColumns;
    protected final int[] rightProjectionColumnsSize;

    // left = 0, right = 1
    private final boolean[] projectionOrder;

    public UniqueHashJoinWithProjection(
            ReadOnlyBufferIndex<IKey> leftIndex,
            ReadOnlyBufferIndex<IKey> rightIndex,
            int[] leftProjectionColumns,
            int[] leftProjectionColumnsSize,
            int[] rightProjectionColumns,
            int[] rightProjectionColumnsSize,
            boolean[] projectionOrder,
            int entrySize) {
        super(entrySize);
        this.leftIndex = leftIndex;
        this.rightIndex = rightIndex;
        this.leftProjectionColumns = leftProjectionColumns;
        this.leftProjectionColumnsSize = leftProjectionColumnsSize;
        this.rightProjectionColumns = rightProjectionColumns;
        this.rightProjectionColumnsSize = rightProjectionColumnsSize;
        this.projectionOrder = projectionOrder;
    }

    public boolean isHashJoin() { return true; }

    public UniqueHashJoinWithProjection asHashJoin() { return this; }

    public MemoryRefNode run(FilterContext leftFilter, FilterContext rightFilter, IKey... keys) {

        IRecordIterator<IKey> iterator = this.leftIndex.iterator(keys);

        while(iterator.hasElement()){
            if(this.leftIndex.checkCondition(iterator, leftFilter) &&
                // do the probing
                this.rightIndex.checkCondition(iterator.get(), rightFilter)) {
                    append( iterator,
                            leftProjectionColumns, leftProjectionColumnsSize,
                            rightProjectionColumns, rightProjectionColumnsSize
                    );

                }
            iterator.next();
        }

        return memoryRefNode;

    }

    public MemoryRefNode run(FilterContext leftFilter, FilterContext rightFilter) {

        IRecordIterator<IKey> outerIterator = this.leftIndex.iterator();

        while(outerIterator.hasElement()){
            if(leftIndex.checkCondition(outerIterator, leftFilter)){
                if(rightIndex.checkCondition(outerIterator.get(), rightFilter)) {
                    append( outerIterator, leftProjectionColumns, leftProjectionColumnsSize,
                            rightProjectionColumns, rightProjectionColumnsSize );

                }
            }
            outerIterator.next();
        }

        return memoryRefNode;
    }

    /**
     * Copy the column values from both relations in the order of projection
     * This is to avoid rechecking procedures by the caller
     * Easier to ensure (implicitly) that remote calls between modules remain consistent
     * just by following conventions
     */
    private void append(IRecordIterator<IKey> iterator,
                        int[] leftProjectionColumns, int[] leftValueSizeInBytes,
                        int[] rightProjectionColumns, int[] rightValueSizeInBytes){

        int leftProjIdx = 0;
        int rightProjIdx = 0;

        Object[] leftRecord = this.leftIndex.record(iterator);
        // not the address of left iterator
        Object[] rightRecord = this.rightIndex.record(iterator.get());

        for(int projOrdIdx = 0; projOrdIdx < projectionOrder.length; projOrdIdx++) {

            // left
            if(!projectionOrder[projOrdIdx]){
                DataTypeUtils.callWriteFunction( this.currentBuffer.address(), this.leftIndex.schema().columnDataType( leftProjIdx ), leftRecord[leftProjectionColumns[leftProjIdx]] );
                this.currentBuffer.forwardOffset(leftValueSizeInBytes[leftProjIdx]);
                leftProjIdx++;
            } else {
                DataTypeUtils.callWriteFunction( this.currentBuffer.address(), this.rightIndex.schema().columnDataType( rightProjIdx ), rightRecord[rightProjectionColumns[rightProjIdx]] );
                this.currentBuffer.forwardOffset(rightValueSizeInBytes[rightProjIdx]);
                rightProjIdx++;
            }

        }

    }

}
