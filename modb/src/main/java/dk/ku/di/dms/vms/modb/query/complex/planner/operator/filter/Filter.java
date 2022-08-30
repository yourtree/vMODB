package dk.ku.di.dms.vms.modb.query.complex.planner.operator.filter;

import java.util.Comparator;

abstract class Filter<V> implements IFilter<V> {

    public final Comparator<V> comparator;

    public Filter(final Comparator<V> comparator) {
        this.comparator = comparator;
    }

}
