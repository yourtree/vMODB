package dk.ku.di.dms.vms.modb.query.planner.operators.sum;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractSimpleOperator;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class Sum extends AbstractSimpleOperator {

    protected final ReadOnlyIndex<IKey> index;

    protected final int columnIndex;

    protected final DataType dataType;

    public Sum(DataType dataType,
                    int columnIndex,
                    ReadOnlyIndex<IKey> index) {
        super(dataType.value);
        this.dataType = dataType;
        this.index = index;
        this.columnIndex = columnIndex;
    }

    protected interface SumOperation<T> extends Consumer<T>, Supplier<T> {
        default int asInt() {
            throw new IllegalStateException("Not applied");
        }
        default float asFloat() {
            throw new IllegalStateException("Not applied");
        }
        default float asDouble() {
            throw new IllegalStateException("Not applied");
        }
        default float asLong() {
            throw new IllegalStateException("Not applied");
        }
    }

    private static class IntSumOp implements SumOperation<Integer> {

        int sum = 0;

        @Override
        public void accept(Integer i) {
            sum += i;
        }

        @Override
        public Integer get() {
            return sum;
        }

        @Override
        public int asInt() {
            return sum;
        }
    }

    private static class FloatSumOp implements SumOperation<Float> {

        float sum = 0;

        @Override
        public void accept(Float i) {
            sum += i;
        }

        @Override
        public Float get() {
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

    @SuppressWarnings("unchecked, rawtypes")
    public MemoryRefNode run(
            FilterContext filterContext){

        SumOperation sumOperation = buildOperation(dataType);

        IRecordIterator<IKey> iterator = index.iterator();
        while(iterator.hasElement()){
            if(index.checkCondition(iterator, filterContext)){
                Object val = index.record(iterator)[columnIndex];
                sumOperation.accept(val);
            }
            iterator.next();
        }

        appendResult(sumOperation);
        return memoryRefNode;

    }

    protected void appendResult(SumOperation<?> sumOperation){

        switch (dataType){
            case INT -> this.currentBuffer.append( sumOperation.asInt() );
            case FLOAT -> this.currentBuffer.append( sumOperation.asFloat() );
            case DOUBLE -> this.currentBuffer.append( sumOperation.asDouble() );
            case LONG -> this.currentBuffer.append( sumOperation.asLong() );
        }

    }

}
