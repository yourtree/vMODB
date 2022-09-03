package dk.ku.di.dms.vms.modb.manipulation.delete;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.query.planner.filter.FilterContext;

/**
 * Modifies the active bit in a record
 */
public class DeleteOperation {

    public final AbstractIndex<IKey> index;

    public DeleteOperation(AbstractIndex<IKey> index) {
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
