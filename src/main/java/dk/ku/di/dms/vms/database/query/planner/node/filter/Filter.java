package dk.ku.di.dms.vms.database.query.planner.node.filter;

import dk.ku.di.dms.vms.database.store.refac.Row;

import java.io.Serializable;
import java.util.Comparator;

public abstract class Filter<V extends Serializable> implements IFilter<Row> {

    public final V fixedValue;

    public final Comparator<V> comparator;

    public final int columnIndex;

    public Filter(final V fixedValue, final Comparator<V> comparator,final int columnIndex) {
        this.fixedValue = fixedValue;
        this.comparator = comparator;
        this.columnIndex = columnIndex;
    }

}
