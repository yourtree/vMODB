package dk.ku.di.dms.vms.modb.query.planner.operators.count;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadOnlyBufferIndex;
import dk.ku.di.dms.vms.modb.query.planner.operators.AbstractSimpleOperator;

public class AbstractCount extends AbstractSimpleOperator {

    protected final ReadOnlyBufferIndex<IKey> index;

    public AbstractCount(ReadOnlyBufferIndex<IKey> index, int entrySize) {
        super(entrySize);
        this.index = index;
    }

    /**
     * Only used by count operator
     * @param count
     */
    protected void append( int count ) {
        ensureMemoryCapacity();
        this.currentBuffer.append(1); // number of rows
        this.currentBuffer.append(count);
    }

}
