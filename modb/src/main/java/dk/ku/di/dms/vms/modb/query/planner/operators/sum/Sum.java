package dk.ku.di.dms.vms.modb.query.planner.operators.sum;

import dk.ku.di.dms.vms.modb.common.meta.DataTypeUtils;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractOperator;
import dk.ku.di.dms.vms.modb.storage.iterator.RecordIterator;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryRefNode;

public class Sum extends AbstractOperator {

    public MemoryRefNode run(){

        int columnOffset = this.index.getTable().getSchema().getColumnOffset(columnIndex);

        if(index.getType() == IndexTypeEnum.UNIQUE){

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

        // non unique
        return null;

    }

}
