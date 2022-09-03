package dk.ku.di.dms.vms.modb.query.planner.operators.join;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;

public class HashJoinWithProjection {

    public HashJoinWithProjection(
            AbstractIndex<IKey> index,
            int[] projectionColumns,
            int[] projectionColumnSize,
            int entrySize) {

    }

}
