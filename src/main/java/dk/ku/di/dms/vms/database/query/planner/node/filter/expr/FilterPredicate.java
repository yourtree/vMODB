package dk.ku.di.dms.vms.database.query.planner.node.filter.expr;

import java.io.Serializable;
import java.util.Comparator;

public class FilterPredicate<V extends Serializable> implements IFilterPredicate<V> {

    public final V fixedValue;
    public final Comparator<V> comparator;

    public FilterPredicate(V fixedValue, Comparator<V> comparator) {
        this.fixedValue = fixedValue;
        this.comparator = comparator;
    }

//    @Override
//    public boolean test(V v) {
//        return comparator.compare(fixedValue, v) > 0;
//    }
}
