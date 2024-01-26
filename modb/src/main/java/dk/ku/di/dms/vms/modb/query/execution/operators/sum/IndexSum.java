package dk.ku.di.dms.vms.modb.query.execution.operators.sum;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyBufferIndex;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

/**
 *
 * On the other side, the sum is type-dependent. Can be done while the records are scanned.
 */
public class IndexSum extends Sum {

    public IndexSum(DataType dataType,
                    int columnIndex,
                    ReadOnlyBufferIndex<IKey> index) {
        super(dataType, columnIndex, index);
    }

    @SuppressWarnings("unchecked, rawtypes")
    public MemoryRefNode run(FilterContext filterContext, IKey... keys){
        SumOperation sumOperation = buildOperation(dataType);
        IRecordIterator<IKey> iterator = this.index.iterator(keys);
        while(iterator.hasElement()){
            if(this.index.checkCondition(iterator, filterContext)){
                sumOperation.accept(this.index.record(iterator)[this.columnIndex]);
            }
            iterator.next();
        }
        appendResult(sumOperation);
        return this.memoryRefNode;
    }

}
