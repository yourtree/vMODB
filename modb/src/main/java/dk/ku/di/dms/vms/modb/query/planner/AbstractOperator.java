package dk.ku.di.dms.vms.modb.query.planner;

import dk.ku.di.dms.vms.modb.common.meta.DataType;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterType;
import dk.ku.di.dms.vms.modb.storage.record.AppendOnlyBuffer;
import dk.ku.di.dms.vms.modb.common.meta.DataTypeUtils;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.storage.memory.MemoryRefNode;

import java.util.List;
import java.util.Map;

public abstract class AbstractOperator {

    // used to uniquely identify this operator in a query plan
    protected final int id;

    // the first node of the memory segment nodes
    protected MemoryRefNode memoryRefNode = null;

    protected AppendOnlyBuffer currentBuffer;

    // dependent on the type of operator
    protected final int entrySize;

    public AbstractOperator(int id, int entrySize) {
        this.id = id;
        this.entrySize = entrySize;
    }

    // just a matter of managing the indexes of both predicate types
    protected boolean checkCondition(long address, FilterContext filterContext, AbstractIndex<IKey> index){

        if(filterContext == null) return true;

        boolean conditionHolds = true;

        // the number of filters to apply
        int filterIdx = 0;

        // the filter index on which a given param (e.g., literals, zero, 1, 'SURNAME', etc) should apply
        int biPredIdx = 0;

        // simple predicates, do not involve input params (i.e, NULL, NOT NULL, EXISTS?, etc)
        int predIdx = 0;

        while( conditionHolds && filterIdx < filterContext.filterTypes.length ){

            // no need to read active bit

            int columnIndex = filterContext.filterColumns[filterIdx];
            int columnOffset = index.getTable().getSchema().getColumnOffset( columnIndex );
            DataType dataType = index.getTable().getSchema().getColumnDataType( columnIndex );

            Object val = DataTypeUtils.getValue( dataType, address + columnOffset );

            // it is a literal passed to the query
            if(filterContext.filterTypes[filterIdx] == FilterType.BP) {
                conditionHolds = filterContext.biPredicates[biPredIdx].
                        apply(val, filterContext.biPredicateParams[biPredIdx]);
                biPredIdx++;
            } else {
                conditionHolds = filterContext.predicates[predIdx].test( val );
                predIdx++;
            }

            filterIdx++;

        }

        return conditionHolds;

    }

    /**
     * Just abstracts on which memory segment a result will be written to
     *
     * Default method. Operators can create their own
     */
    protected void ensureMemoryCapacity(){

        if(currentBuffer.capacity() - currentBuffer.address() > entrySize){
            return;
        }

        // else, get a new memory segment
        MemoryRefNode claimed = MemoryManager.claim();

        claimed.next = memoryRefNode;
        memoryRefNode = claimed;

        this.currentBuffer = new AppendOnlyBuffer(claimed.address(), claimed.bytes());

    }

    /**
     *
     */
    protected void append( List<Map.Entry<long, DataType>> values ) {
        ensureMemoryCapacity();
        this.currentBuffer.append(values);
    }

}
