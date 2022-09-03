package dk.ku.di.dms.vms.modb.query.planner.operators.sum;

import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.storage.memory.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractOperator;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordIterator;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryRefNode;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class Sum extends AbstractOperator {

    protected final AbstractIndex<IKey> index;

    protected final FilterContext filterContext;

    protected final int columnIndex;

    protected final DataType dataType;

    protected final SumOperation<Object> sumOperation;

    @SuppressWarnings("unchecked")
    public Sum(DataType dataType,
                    int columnIndex,
                    AbstractIndex<IKey> index,
                    FilterContext filterContext) {
        super(dataType.value);
        this.dataType = dataType;
        this.index = index;
        this.filterContext = filterContext;
        this.columnIndex = columnIndex;
        this.sumOperation = (SumOperation<Object>) buildOperation(dataType);
    }

    protected interface SumOperation<T> extends Consumer<T>, Supplier<T> {
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

        @Override
        public float asFloat() {
            return sum;
        }
    }

    // we need a class that performs the  operation and maintains state
    // the interface should have a close() method. if sum, return. if count (have to consider distinct), return. if
    // a consumer may suffice. srcAddress, columnOffset
    // dynamically create. different from filter, this must maintain state, ok to create for each query

    SumOperation<?> buildOperation(DataType dataType){

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

        RecordIterator iterator = index.asUniqueHashIndex().iterator();
        long address;
        while(iterator.hasNext()){
            address = iterator.next();
            if(checkCondition(address, filterContext, index)){
                Object val = DataTypeUtils.getValue( dataType, address + columnOffset );
                sumOperation.accept(val);
            }
        }

        appendResult();
        return memoryRefNode;

    }

    protected void appendResult(){

        switch (dataType){
            case INT -> this.currentBuffer.append( sumOperation.asInt() );
            case FLOAT -> this.currentBuffer.append( sumOperation.asFloat() );
        }

    }

}
