package dk.ku.di.dms.vms.modb.query.planner.operators.sum;

import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.storage.memory.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordBucketIterator;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryRefNode;

/**
 *
 * On the other side, the sum is type-dependent. Can be done while the records are scanned.
 */
public class IndexSum extends Sum {

    public IndexSum(DataType dataType,
                    int columnIndex,
                    AbstractIndex<IKey> index) {
        super(dataType, columnIndex, index);
    }

    public MemoryRefNode run(FilterContext filterContext, IKey[] keys){

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
        NonUniqueHashIndex cIndex = index.asNonUniqueHashIndex();
        long address;
        for(IKey key : keys){
            RecordBucketIterator iterator = cIndex.iterator(key);
            while(iterator.hasNext()){

                address = iterator.next();

                if(checkCondition(address, filterContext, index)){
                    Object val = DataTypeUtils.getValue( dataType, address + columnOffset );
                    sumOperation.accept(val);
                }

            }
        }

        appendResult();
        return memoryRefNode;

    }

}
