package dk.ku.di.dms.vms.database.query.planner.node.filter;

import java.util.Comparator;

public abstract class Filter<V> implements IDynamicFilter<V> {

    public V fixedValue;

    public final Comparator<V> comparator;

    public Filter(final V fixedValue, final Comparator<V> comparator) {
        this.fixedValue = fixedValue;
        this.comparator = comparator;
    }

    @Override
    public void redefine(V newFixedValue) {
        this.fixedValue = newFixedValue;
    }

}
