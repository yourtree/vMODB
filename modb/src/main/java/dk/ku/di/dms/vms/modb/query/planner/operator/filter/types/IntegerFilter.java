package dk.ku.di.dms.vms.modb.query.planner.operator.filter.types;

import dk.ku.di.dms.vms.modb.query.planner.operator.filter.IFilter;

import java.util.Comparator;

public class IntegerFilter implements IFilter<Integer> {

    private final Comparator<Integer> comparator;

    public IntegerFilter(Comparator<Integer> comparator) {
        this.comparator = comparator;
    }

    @Override
    public boolean eval(Integer x, Integer y) {
        return this.comparator.compare( x, y ) == 0;
    }
}
