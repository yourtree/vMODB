package dk.ku.di.dms.vms.database.query.planner.node.filter;

import dk.ku.di.dms.vms.database.store.meta.DataType;

import java.util.Comparator;

public abstract class Filter<V> implements IDynamicFilter<V> {

    /**
     * To be used by the scan operator
     */
    public final DataType dataType;

    public V fixedValue;

    public final Comparator<V> comparator;

    public Filter(final DataType dataType, final V fixedValue, final Comparator<V> comparator) {
        this.dataType = dataType;
        this.fixedValue = fixedValue;
        this.comparator = comparator;
    }

    @Override
    public void redefine(V newFixedValue) {
        this.fixedValue = newFixedValue;
    }

}
