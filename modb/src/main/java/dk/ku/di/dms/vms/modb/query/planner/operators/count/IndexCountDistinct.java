package dk.ku.di.dms.vms.modb.query.planner.operators.count;

import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.IndexTypeEnum;
import dk.ku.di.dms.vms.modb.index.ReadOnlyIndex;
import dk.ku.di.dms.vms.modb.index.non_unique.NonUniqueHashIndex;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;

import java.util.HashMap;
import java.util.Map;

/**
 * No projecting any other column for now
 *
 * To make reusable, internal state must be made ephemeral
 */
public class IndexCountDistinct extends AbstractCount {

    private static class EphemeralState {
        private int count;
        // hashed by the values in the distinct clause
        private final Map<Integer,Integer> valuesSeen;

        private EphemeralState() {
            this.count = 0;
            this.valuesSeen = new HashMap<Integer,Integer>();
        }
    }

    public IndexCountDistinct(ReadOnlyIndex<IKey> index
                               ) {
        super(index, Integer.BYTES);
    }

    /**
     * Can be reused across different distinct columns
     */
    public MemoryRefNode run(int distinctColumnIndex, FilterContext filterContext, IKey... keys){

        EphemeralState state = new EphemeralState();

        DataType dt = this.index.schema().getColumnDataType(distinctColumnIndex);

        if(index.getType() == IndexTypeEnum.UNIQUE){

            UniqueHashIndex cIndex = index.asUniqueHashIndex();
            long address;
            for(IKey key : keys){
                address = cIndex.retrieve(key);

                if(index.checkCondition(key, address, filterContext)){

                    address = index.getColumnAddress(key, address, distinctColumnIndex);
                    Object val = DataTypeUtils.getValue( dt, address );
                    if( !state.valuesSeen.containsKey(val.hashCode())) {
                        state.count++;
                        state.valuesSeen.put(val.hashCode(), 1);
                    }
                }
            }

            append(state.count);
            return memoryRefNode;

        }

        // non unique
        NonUniqueHashIndex cIndex = index.asNonUniqueHashIndex();
        long address;
        for(IKey key : keys){
            IRecordIterator iterator = cIndex.iterator(key);
            while(iterator.hasNext()){

                if(index.checkCondition(iterator, filterContext)) {
                    address = index.getColumnAddress(iterator, distinctColumnIndex);
                    Object val = DataTypeUtils.getValue( dt, address );
                    if( !state.valuesSeen.containsKey(val.hashCode())) {
                        state.count++;
                        state.valuesSeen.put(val.hashCode(), 1);
                    }
                }

                iterator.next();

            }
        }

        append(state.count);
        return memoryRefNode;

    }

}
