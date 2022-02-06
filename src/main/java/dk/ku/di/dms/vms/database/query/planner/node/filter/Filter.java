package dk.ku.di.dms.vms.database.query.planner.node.filter;

import java.io.Serializable;
import java.util.Comparator;

public abstract class Filter<V extends Serializable> implements IFilter<V> {

    public final V fixedValue;

    public final Comparator<V> comparator;

    public Filter(final V fixedValue, final Comparator<V> comparator) {
        this.fixedValue = fixedValue;
        this.comparator = comparator;
    }

}
