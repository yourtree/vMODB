package dk.ku.di.dms.vms.modb.query.planner.operators.join;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.index.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractOperator;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;

public class HashJoinWithProjection extends AbstractOperator {

    public final ReadOnlyIndex<IKey> leftIndex;
    public final ReadOnlyIndex<IKey> rightIndex;

    // index of the columns
    protected final int[] leftProjectionColumns;
    protected final int[] leftProjectionColumnsSize;

    protected final int[] rightProjectionColumns;
    protected final int[] rightProjectionColumnsSize;

    // left = 0, right = 1
    private final boolean[] projectionOrder;

    public HashJoinWithProjection(
            AbstractIndex<IKey> leftIndex,
            AbstractIndex<IKey> rightIndex,
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

    public HashJoinWithProjection asHashJoin() { return this; }

    public MemoryRefNode run(FilterContext leftFilter, FilterContext rightFilter, IKey... keys) {

        UniqueHashIndex outerIndex = leftIndex.asUniqueHashIndex();
        UniqueHashIndex innerIndex = rightIndex.asUniqueHashIndex();
        long outerAddress;
        long innerAddress;

        for(IKey key : keys){

            outerAddress = outerIndex.retrieve(key);

            if(leftIndex.checkCondition(key, leftFilter)){

                innerAddress = innerIndex.retrieve(key);

                if(rightIndex.checkCondition(key, rightFilter)) {

                    append( outerAddress, leftProjectionColumns, this.leftIndex.schema().columnOffset(), leftProjectionColumnsSize,
                            innerAddress, rightProjectionColumns, this.rightIndex.schema().columnOffset(), rightProjectionColumnsSize
                    );

                }

            }

        }

        return memoryRefNode;

    }

    public MemoryRefNode run(FilterContext leftFilter, FilterContext rightFilter) {

        IRecordIterator outerIterator = leftIndex.asUniqueHashIndex().iterator();
        UniqueHashIndex innerIndex = rightIndex.asUniqueHashIndex();
        long outerAddress;
        long innerAddress;

        while(outerIterator.hasNext()){

            if(leftIndex.checkCondition(outerIterator, leftFilter)){

                outerAddress = outerIterator.current();
                IKey outerKey = outerIterator.primaryKey();
                innerAddress = innerIndex.retrieve(outerKey);

                if(rightIndex.checkCondition(outerKey, rightFilter)) {

                    append( outerAddress, leftProjectionColumns, this.leftIndex.schema().columnOffset(), leftProjectionColumnsSize,
                            innerAddress, rightProjectionColumns, this.rightIndex.schema().columnOffset(), rightProjectionColumnsSize
                            );

                }

            }

        }

        return memoryRefNode;
    }

    /**
     * Copy the column values from both relations in the order of projection
     * This is to avoid rechecking procedures by the caller
     * Easier to ensure (implicitly) that remote calls between modules remain consistent
     * just by following conventions
     */
    private void append(long leftSrcAddress, int[] leftProjectionColumns, int[] leftColumnOffset, int[] leftValueSizeInBytes,
                        long rightSrcAddress, int[] rightProjectionColumns, int[] rightColumnOffset, int[] rightValueSizeInBytes){

        int leftProjIdx = 0;
        int rightProjIdx = 0;

        // get direct byte buffer
//        ByteBuffer bb = MemoryManager.getTemporaryDirectBuffer(entrySize);
//        long bbAddress = MemoryUtils.getByteBufferAddress(bb);

        for(int projOrdIdx = 0; projOrdIdx < projectionOrder.length; projOrdIdx++) {

            // left
            if(!projectionOrder[projOrdIdx]){
                this.currentBuffer.append( leftSrcAddress,
                        leftColumnOffset[leftProjectionColumns[leftProjIdx]],
                        leftValueSizeInBytes[leftProjIdx]);
                leftProjIdx++;
            } else {
                this.currentBuffer.append( rightSrcAddress,
                        rightColumnOffset[rightProjectionColumns[rightProjIdx]],
                        rightValueSizeInBytes[rightProjIdx]);
                rightProjIdx++;
            }

        }

    }

}
