package dk.ku.di.dms.vms.modb.query.planner.operators.join;

import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
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

    // this is unnecessary, since it can be get from the index directly
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

            if(leftIndex.checkCondition(key, outerAddress, leftFilter)){

                innerAddress = innerIndex.retrieve(key);

                if(rightIndex.checkCondition(key, innerAddress, rightFilter)) {

                    append( key, outerAddress, leftProjectionColumns, leftProjectionColumnsSize,
                            innerAddress, rightProjectionColumns, rightProjectionColumnsSize
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

                if(rightIndex.checkCondition(outerKey, outerAddress, rightFilter)) {

                    append( outerKey, outerAddress, leftProjectionColumns, leftProjectionColumnsSize,
                            innerAddress, rightProjectionColumns, rightProjectionColumnsSize
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
    private void append(IKey key, long leftSrcAddress, int[] leftProjectionColumns, int[] leftValueSizeInBytes,
                        long rightSrcAddress, int[] rightProjectionColumns, int[] rightValueSizeInBytes){

        int leftProjIdx = 0;
        int rightProjIdx = 0;

        Object[] leftRecord = this.leftIndex.readFromIndex(key, leftSrcAddress);
        Object[] rightRecord = this.rightIndex.readFromIndex(key, rightSrcAddress);

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
