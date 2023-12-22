package dk.ku.di.dms.vms.modb.manipulation.delete;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractBufferedIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;

/**
 * Modifies the active bit in a record
 */
public class DeleteOperation {

    public final AbstractBufferedIndex<IKey> index;

    public DeleteOperation(AbstractBufferedIndex<IKey> index) {
        this.index = index;
    }

    // index
    public void run(FilterContext filterContext, IKey... keys){

        // must touch the tx pool first?

    }

    // no index
    public void run(FilterContext filterContext){

    }

}
