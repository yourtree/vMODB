package dk.ku.di.dms.vms.database.query.planner.node.filter;

import java.io.Serializable;

public class Filter<V extends Serializable> implements IFilter<V> {

    public V fixedValue;

    public int columnIdx;

    // public final Comparator<V> comparator;

    public final FilterBuilder.IComparator<V> comparator;

    public Filter(final V fixedValue, final int columnIdx,
                  final FilterBuilder.IComparator<V> comparator) {
        this.fixedValue = fixedValue;
        this.columnIdx = columnIdx;
        this.comparator = comparator;
    }

    public Filter(final FilterBuilder.IComparator<V> comparator) {
        this.fixedValue = null;
        //this.columnIdx;
        this.comparator = comparator;
    }

    /** to avoid creating again the same filter */
    public final void redefine(final V fixedValue, final int columnIdx){
        this.fixedValue = fixedValue;
        this.columnIdx = columnIdx;
    }

    @Override
    public boolean test(V v) {
        return comparator.compare(fixedValue,v);
    }
}
