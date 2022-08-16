package dk.ku.di.dms.vms.modb.query.planner.operator.filter.refac;

import dk.ku.di.dms.vms.modb.query.planner.operator.filter.types.TypedBiPredicate;

import java.util.function.Predicate;

public class FilterContext {

    public FilterContextBuilder.FilterType[] filterTypes;

    public TypedBiPredicate<?>[] biPredicates;

    // only applied to null
    public Predicate<?>[] predicates;

    public int[] filterColumns;

    // case of literals
    public Object[] filterParams;


}
