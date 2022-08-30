package dk.ku.di.dms.vms.modb.query.planner.filter;

import dk.ku.di.dms.vms.modb.query.planner.filter.types.TypedBiPredicate;

import java.util.function.Predicate;

public class FilterContext {

    public FilterType[] filterTypes;

    public int[] filterColumns; // allow querying the schema to get the corresponding data type

    public TypedBiPredicate<Object>[] biPredicates;

    // case of literals passed to the query
    // transient, may change on every application code call
    public Object[] biPredicateParams;

    // only applied to null
    public Predicate<Object>[] predicates;

}
