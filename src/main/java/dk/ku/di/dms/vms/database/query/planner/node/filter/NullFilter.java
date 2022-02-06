package dk.ku.di.dms.vms.database.query.planner.node.filter;

import java.io.Serializable;

public class NullFilter<V extends Serializable> implements IFilter<V> {

    @Override
    public boolean test(V v) {
        return v == null;
    }
}
