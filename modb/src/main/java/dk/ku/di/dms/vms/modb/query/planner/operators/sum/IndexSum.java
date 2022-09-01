package dk.ku.di.dms.vms.modb.query.planner.operators.sum;

import dk.ku.di.dms.vms.modb.common.meta.DataType;
import dk.ku.di.dms.vms.modb.common.meta.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractOperator;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryRefNode;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 *
 * On the other side, the sum is type-dependent. Can be done while the records are scanned.
 */
public class IndexSum extends AbstractOperator {

    private final AbstractIndex<IKey> index;

    private final FilterContext filterContext;

    private final IKey[] keys;

    private final int columnIndex;

    private final DataType dataType;

    private final SumOperation<Object> sumOperation;

    @SuppressWarnings("unchecked")
    public IndexSum(int id,
                    DataType dataType,
                    int columnIndex,
                    AbstractIndex<IKey> index,
                    FilterContext filterContext,
                    IKey... keys) {
        super(id, dataType.value);
        this.dataType = dataType;
        this.index = index;
        this.filterContext = filterContext;
        this.columnIndex = columnIndex;
        this.keys = keys;
        this.sumOperation = (SumOperation<Object>) buildOperation(dataType);
    }

    private interface SumOperation<T> extends Consumer<T>, Supplier<T> {
        default int asInt() {
            throw new IllegalStateException("Not applied");
        }
        default float asFloat() {
            throw new IllegalStateException("Not applied");
        }
    }

    private static class IntSumOp implements SumOperation<int> {

        int sum = 0;

        @Override
        public void accept(int i) {
            sum += i;
        }

        @Override
        public int get() {
            return sum;
        }

        @Override
        public int asInt() {
            return sum;
        }
    }

    private static class FloatSumOp implements SumOperation<float> {

        float sum = 0;

        @Override
        public void accept(float i) {
            sum += i;
        }

        @Override
        public float get() {
            return sum;
        }

        public float asFloat() {
            return sum;
        }
    }

    // we need a class that performs the  operation and maintains state
    // the interface should have a close() method. if sum, return. if count (have to consider distinct), return. if
    // a consumer may suffice. srcAddress, columnOffset
    // dynamically create. different from filter, this must maintain state, ok to create for each query

    private SumOperation<?> buildOperation(DataType dataType){

        switch(dataType){
            case INT -> {
                return new IntSumOp();
            }
            case FLOAT -> {
                return new FloatSumOp();
            }
        }

        return null;
    }

    public MemoryRefNode run(){

        int columnOffset = this.index.getTable().getSchema().getColumnOffset(columnIndex);

        if(index.getType() == IndexTypeEnum.UNIQUE){

            UniqueHashIndex cIndex = index.asUniqueHashIndex();
            long address;
            for(IKey key : keys){
                address = cIndex.retrieve(key);
                if(checkCondition(address, filterContext, index)){

                    Object val = DataTypeUtils.getValue( dataType, address + columnOffset );
                    sumOperation.accept(val);

                }
            }

            appendResult();
            return memoryRefNode;

        }

        // non unique
        return null;

    }

    private void appendResult(){

        switch (dataType){
            case INT -> this.currentBuffer.append( sumOperation.asInt() );
            case FLOAT -> this.currentBuffer.append( sumOperation.asFloat() );
        }

    }

}
